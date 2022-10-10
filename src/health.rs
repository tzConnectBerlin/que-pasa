use std::thread;
use warp::{Filter, http::StatusCode};

pub(crate) fn spawn_api(port: u16) {
    log::info!("spawning the health api on port {}", port);

    thread::spawn(move || {
       tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            run_server(port).await;
        })
    });
}

async fn run_server(port: u16) {
    let health = warp::path::end()
        .map(|| StatusCode::OK);

    warp::serve(health)
        .run(([0, 0, 0, 0], port))
        .await;
}
