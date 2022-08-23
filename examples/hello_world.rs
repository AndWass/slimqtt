use slimqtt::session::*;
use std::time::Duration;

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new().init().unwrap();
    let stream = tokio::net::TcpStream::connect("test.mosquitto.org:1883")
        .await
        .unwrap();
    let (mut session, mut task) = Session::new(
        stream,
        SessionConfig {
            client_id: "11231abc".to_string(),
            keep_alive: Duration::from_secs(30),
            login: None,
            clean_session: true,
        },
    );
    log::info!("{:?}", task.run().await);
}
