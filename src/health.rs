use std::thread;
use warp::{Filter, Rejection, Reply, reject, http::StatusCode};
use tokio_postgres::{NoTls, Error};

use crate::sql::db::DBClient;

#[derive(Debug)]
struct RejectionErr {
    pub err: String,
}
impl reject::Reject for RejectionErr {}

pub(crate) fn spawn_api(port: u16, database_url: String, main_schema: String) {
    // thread::spawn(move || {
    // ....///.[`LocalSet`]:.crate::task::LocalSet¬
       tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            run_server(port, database_url, main_schema).await;
        })
    // });
}

async fn run_server(port: u16, database_url: String, main_schema: String) {
    let health = warp::path!("health")
        .and(warp::any().map(move || database_url.clone()))
        .and(warp::any().map(move || main_schema.clone()))
        .and_then(health_handler);

    warp::serve(health)
        .run(([0, 0, 0, 0], port))
        .await;
}

async fn health_handler(database_url: String, main_schema: String) -> std::result::Result<impl Reply, Rejection> {
    log::info!("1");
    let (client, connection) =
        tokio_postgres::connect(&database_url, NoTls).await.map_err(|e| reject::custom(RejectionErr { err: format!("{}", e) }))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    log::info!("2");
    client.simple_query(
        format!(r#"SET SCHEMA '{}'"#, main_schema).as_str(),
    ).await.map_err(|e| reject::custom(RejectionErr { err: format!("{}", e) }))?;

    log::info!("3");
    let rows = client.query("SELECT baked_at FROM levels", &[]).await.map_err(|e| reject::custom(RejectionErr { err: format!("{}", e) }))?;
    let value: chrono::DateTime<chrono::Utc> = rows[0].get(0);

    log::info!("4");
    log::info!("Hello! {:?}", value);

    Ok(StatusCode::OK)
}
