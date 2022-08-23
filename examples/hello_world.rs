use std::time::Duration;

use slimqtt::session::*;

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new().init().unwrap();
    let stream = tokio::net::TcpStream::connect("test.mosquitto.org:1883")
        .await
        .unwrap();
    let mut config = SessionConfig::new("slimqtt-hello-world");
    config.set_keep_alive(Duration::from_secs(30));
    let (mut session, mut task) = Session::new(stream, config);
    tokio::spawn(async move { task.run().await });
    for x in 1..=10 {
        println!(
            "Publish result: {:?}",
            session
                .publish("/slimqtt", format!("Hello world {}", x))
                .await
        );
    }
}
