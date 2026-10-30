use crate::setup::{connection_sqlx_direct, connections_tokio};
use sqlx::{Executor, Pool, Postgres};
use tokio_postgres::{Client, SimpleQueryMessage};

fn command_tags(messages: &[SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|message| matches!(message, SimpleQueryMessage::CommandComplete(_)))
        .count()
}

async fn count(direct: &Pool<Postgres>, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(direct)
        .await
        .unwrap()
}

async fn setup(table: &str) -> (Client, Pool<Postgres>) {
    let direct = connection_sqlx_direct().await;
    direct
        .execute(format!("DROP TABLE IF EXISTS {table}").as_str())
        .await
        .unwrap();
    direct
        .execute(format!("CREATE TABLE {table} (id BIGINT)").as_str())
        .await
        .unwrap();

    // Single-shard database.
    let conn = connections_tokio().await.swap_remove(0);

    (conn, direct)
}

#[tokio::test]
async fn test_begin_multi_statement_runs_every_statement() {
    let table = "multi_stmt_txn_begin";
    let (conn, direct) = setup(table).await;

    let messages = conn
        .simple_query(&format!("BEGIN; INSERT INTO {table} VALUES (1); COMMIT"))
        .await
        .unwrap();
    assert_eq!(command_tags(&messages), 3);
    assert_eq!(count(&direct, table).await, 1);

    // Transaction left open by the query stays open.
    let messages = conn
        .simple_query(&format!(
            "START TRANSACTION; INSERT INTO {table} VALUES (2)"
        ))
        .await
        .unwrap();
    assert_eq!(command_tags(&messages), 2);
    assert_eq!(count(&direct, table).await, 1);
    conn.simple_query("ROLLBACK").await.unwrap();
    assert_eq!(count(&direct, table).await, 1);

    direct
        .execute(format!("DROP TABLE {table}").as_str())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_begin_multi_statement_returns_error() {
    let table = "multi_stmt_txn_error";
    let (conn, direct) = setup(table).await;

    let err = conn
        .simple_query("BEGIN; INSERT INTO multi_stmt_txn_no_such_table VALUES (1); COMMIT")
        .await
        .unwrap_err();
    assert_eq!(
        err.as_db_error().map(|err| err.code().code()),
        Some("42P01")
    );

    // The connection is still usable.
    conn.simple_query("SELECT 1").await.unwrap();

    direct
        .execute(format!("DROP TABLE {table}").as_str())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_end_transaction_multi_statement_runs_every_statement() {
    let table = "multi_stmt_txn_end";
    let (conn, direct) = setup(table).await;

    for query in ["ROLLBACK", "COMMIT"] {
        let messages = conn
            .simple_query(&format!("{query}; INSERT INTO {table} VALUES (1)"))
            .await
            .unwrap();
        assert_eq!(command_tags(&messages), 2);
    }
    assert_eq!(count(&direct, table).await, 2);

    direct
        .execute(format!("DROP TABLE {table}").as_str())
        .await
        .unwrap();
}
