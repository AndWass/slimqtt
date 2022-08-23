use std::time::Duration;

use slimqtt::session::*;

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new().init().unwrap();
    let stream = tokio::net::TcpStream::connect("test.mosquitto.org:1883")
        .await
        .unwrap();
    let mut config = SessionConfig::new("slimqtt-hello-world");
    config
        .set_keep_alive(Duration::from_secs(30))
        .set_last_will(Some(LastWill::new(
            "/hello-slimqtt",
            b"Hello world".to_vec(),
            QoS::AtLeastOnce,
            false,
        )));
    let (mut session, mut task) = Session::new(stream, config);
    log::info!("{:?}", task.run().await);
}
