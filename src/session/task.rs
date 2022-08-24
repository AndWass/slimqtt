use futures::{Sink, SinkExt, TryStreamExt};
use mqttbytes::v4::{Packet, Publish};
use mqttbytes::QoS;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::session::packet_id::{WaitAckSlots, WaitingAck};
use crate::session::{Error, PublishError, SessionConfig};

use super::keep_alive::*;

struct MqttStream<T> {
    stream: crate::codec::Framed<T>,
}

impl<T: Unpin + AsyncRead + AsyncWrite> MqttStream<T> {
    pub fn new(stream: T) -> Self {
        Self {
            stream: crate::codec::Framed::new(stream),
        }
    }

    pub async fn next(&mut self) -> Result<Packet, Error> {
        let packet = self.stream.try_next().await?;
        packet.ok_or(Error::ConnectionClosed)
    }

    pub async fn send<Item>(
        &mut self,
        item: Item,
    ) -> Result<(), <crate::codec::Framed<T> as Sink<Item>>::Error>
    where
        crate::codec::Framed<T>: Sink<Item>,
    {
        self.stream.send(item).await?;
        Ok(())
    }
}

enum State {
    SendConnect,
    WaitingConnAck,
    Connected,
    NeedReset,
}

#[derive(Debug)]
pub struct CommandResultRx<T, E>(pub tokio::sync::oneshot::Receiver<Result<T, E>>);

#[derive(Debug)]
pub struct CommandResultTx<T, E>(pub tokio::sync::oneshot::Sender<Result<T, E>>);

#[derive(Debug)]
pub(crate) enum TaskCommand {
    Publish(Publish, CommandResultTx<(), PublishError>),
}

pub struct SessionTask<T> {
    stream: MqttStream<T>,
    state: State,
    keep_alive: KeepAliveTimer,
    config: SessionConfig,
    session_events: tokio::sync::mpsc::Receiver<TaskCommand>,
    ack_slots: WaitAckSlots,
}

impl<T: Unpin + AsyncRead + AsyncWrite> SessionTask<T> {
    pub(crate) fn new(
        stream: T,
        config: SessionConfig,
    ) -> (Self, tokio::sync::mpsc::Sender<TaskCommand>) {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        (
            Self {
                stream: MqttStream::new(stream),
                state: State::SendConnect,
                keep_alive: KeepAliveTimer::new(config.keep_alive()),
                config,
                session_events: rx,
                ack_slots: WaitAckSlots::new(32),
            },
            tx,
        )
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let res = self.run_inner().await;
        self.state = State::NeedReset;
        res
    }

    async fn run_inner(&mut self) -> Result<(), Error> {
        loop {
            match self.state {
                State::SendConnect => {
                    log::debug!("Sending CONNECT");
                    self.stream.send(self.config.as_connect()).await?;
                    self.keep_alive.reset();
                    self.state = State::WaitingConnAck;
                }
                State::WaitingConnAck => {
                    self.wait_connack().await?;
                    self.state = State::Connected;
                }
                State::Connected => {
                    return self.handle_connected_state().await;
                }
                State::NeedReset => {
                    log::debug!("Attempting to run a session without resetting it first");
                    return Err(Error::NeedReset);
                }
            }
        }
    }

    async fn wait_connack(&mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                Ok(frame) = self.stream.next() => {
                    return match frame {
                        Packet::ConnAck(x) => {
                            log::debug ! ("CONNACK received: {:?}", x.code);
                            self.keep_alive.reset();

                            if x.code == mqttbytes::v4::ConnectReturnCode::Success {
                                Ok(())
                            } else {
                                Err(Error::ConnectionRejected(x.code))
                            }
                        }
                        x => {
                            Err(Error::NotConnack(x))
                        }
                    }
                },
                ping = self.keep_alive.wait() => {
                    if matches!(ping, KeepAlive::PingResponseDeadline) {
                        return Err(Error::KeepAliveTimeout);
                    }
                },
                else => {
                    return Err(Error::ConnectionClosed)
                }
            }
        }
    }

    async fn handle_connected_state(&mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                packet = self.stream.next() => {
                    self.handle_connected_server_packet(packet?).await?;
                },
                keep_alive = self.keep_alive.wait() => {
                    self.handle_connected_keep_alive(keep_alive).await?;
                },
                event = self.session_events.recv() => {
                    self.handle_connected_session_event(event.ok_or(Error::UserStop)?).await?;
                }
            }
        }
    }

    async fn handle_connected_server_packet(&mut self, packet: Packet) -> Result<(), Error> {
        use mqttbytes::v4::*;
        self.keep_alive.reset();
        match packet {
            Packet::Publish(publish) => {
                log::debug!("Received {:?}", publish);
                if publish.qos == QoS::AtLeastOnce {
                    log::debug!("Sending PUBACK for {}", publish.pkid);
                    self.stream.send(&PubAck::new(publish.pkid)).await?;
                } else if publish.qos == QoS::ExactlyOnce {
                    log::debug!("Sending PUBREC for {}", publish.pkid);
                    self.stream.send(&PubRec::new(publish.pkid)).await?;
                }
            }
            Packet::PubAck(ack) => match self.ack_slots.take_puback(ack.pkid) {
                Some(x) => {
                    log::debug!("PUBACK for {} received", ack.pkid);
                    let _ = x.set_puback_result(Ok(()));
                }
                _ => log::warn!("Received PUBACK for unknown packet id {}", ack.pkid),
            },
            Packet::PubRel(pubrel) => {
                log::debug!("PUBREL received: {:?}", pubrel);
                self.stream.send(&PubComp::new(pubrel.pkid)).await?;
            }
            Packet::Disconnect => {
                log::debug!("DISCONNECT received");
                return Err(Error::ConnectionClosed);
            }
            x => {
                log::debug!("Received {:?}", x);
            }
        }

        Ok(())
    }

    async fn handle_connected_keep_alive(&mut self, keep_alive: KeepAlive) -> Result<(), Error> {
        match keep_alive {
            KeepAlive::PingRequest => {
                log::debug!("Writing ping request");
                self.stream.send(&mqttbytes::v4::PingReq).await?;
                Ok(())
            }
            KeepAlive::PingResponseDeadline => {
                log::debug!("Ping response deadline reached");
                Err(Error::KeepAliveTimeout)
            }
        }
    }

    async fn handle_connected_session_event(&mut self, event: TaskCommand) -> Result<(), Error> {
        match event {
            TaskCommand::Publish(publish, response) => self.publish(publish, response).await,
        }
    }

    async fn publish(
        &mut self,
        mut publish: Publish,
        response: CommandResultTx<(), PublishError>,
    ) -> Result<(), Error> {
        log::debug!(
            "Publishing {} bytes to {}",
            publish.payload.len(),
            publish.topic
        );

        if publish.qos == QoS::AtMostOnce {
            self.publish_qos0(publish, response).await
        } else if publish.qos == QoS::AtLeastOnce {
            match self.ack_slots.try_insert(WaitingAck::PubAck(response)) {
                Ok(packet_id) => {
                    publish.pkid = packet_id;
                    self.publish_qos1(publish).await?;
                }
                Err(e) => e.set_puback_result(Err(PublishError::NoPacketIdAvailable)),
            }
            Ok(())
        } else {
            log::warn!("Unsupported QoS");
            let _ = response.0.send(Err(PublishError::Unspecified(
                "Unsupported QoS".to_string(),
            )));
            Ok(())
        }
    }

    async fn publish_qos0(
        &mut self,
        publish: Publish,
        response: CommandResultTx<(), PublishError>,
    ) -> Result<(), Error> {
        log::debug!("Sending QoS0 publish");
        let res = self.stream.send(publish).await;
        let to_respond = match &res {
            Ok(_) => Ok(()),
            Err(e) => Err(PublishError::from(e)),
        };

        let _ = response.0.send(to_respond);

        res?;
        Ok(())
    }

    /// Publish a QoS1 message
    ///
    /// This function assumes that a correct packet ID has been set in the publish message and that
    /// a waiting ACK slot has been filled with a correct waiter.
    async fn publish_qos1(&mut self, publish: Publish) -> Result<(), Error> {
        let packet_id = publish.pkid;
        let res = self.stream.send(publish).await;
        match &res {
            Ok(_) => {
                log::debug!("Sent PUBLISH with packet id {}", packet_id);
            }
            Err(e) => {
                log::warn!("Failed to send PUBLISH: {:?}", e);
                let waiter = self.ack_slots.take_puback(packet_id).unwrap();
                waiter.set_puback_result(Err(PublishError::from(e)));
            }
        }
        // Any error from the "send" above should be returned upwards
        res?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};
    use mqttbytes::v4::{ConnAck, Connect, ConnectReturnCode, Packet, PubAck, Publish};
    use mqttbytes::{v4, QoS};
    use tokio::io::DuplexStream;
    use tokio::sync::mpsc::Sender;
    use tokio::task::JoinHandle;

    use crate::codec::TryAs;
    use crate::session::task::{CommandResultTx, TaskCommand};
    use crate::session::{Error, SessionConfig};

    use super::SessionTask;

    struct TestSession {
        stream: crate::codec::Framed<DuplexStream>,
        join: JoinHandle<Result<(), Error>>,
        task_command: Sender<TaskCommand>,
    }

    impl TestSession {
        pub async fn handshake(&mut self) {
            let _: Connect = self.stream.next().await.unwrap().unwrap().try_as().unwrap();
            self.send_connack_success().await;
        }
        pub async fn send_connack_success(&mut self) {
            self.stream
                .send(ConnAck {
                    session_present: false,
                    code: ConnectReturnCode::Success,
                })
                .await
                .unwrap();
        }
    }

    fn make_session(config: SessionConfig) -> TestSession {
        let (s, stream) = tokio::io::duplex(256);
        let (mut task, evt_tx) = SessionTask::new(s, config);
        let join = tokio::spawn(async move { task.run().await });

        TestSession {
            stream: crate::codec::Framed::new(stream),
            join,
            task_command: evt_tx,
        }
    }

    #[tokio::test]
    async fn first_message_is_connect() {
        let mut session = make_session(SessionConfig::new("hello-world"));

        let message = session.stream.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            Packet::Connect(Connect {
                clean_session: false,
                keep_alive: 5 * 60,
                client_id: "hello-world".to_string(),
                login: None,
                protocol: mqttbytes::Protocol::V4,
                last_will: None
            })
        );
    }

    #[tokio::test]
    async fn disconnect_on_not_connack() {
        use v4::*;
        let packets = [
            Packet::PingResp,
            Packet::PingReq,
            Packet::Connect(Connect::new("hello")),
            Packet::Disconnect,
            Packet::PubAck(PubAck::new(10)),
            Packet::PubComp(PubComp::new(10)),
            Packet::PubRec(PubRec::new(10)),
            Packet::PubRel(PubRel::new(10)),
            Packet::Publish(Publish {
                pkid: 10,
                payload: Bytes::from_static(&[1]),
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: "/hello".to_string(),
            }),
            Packet::Subscribe(Subscribe {
                pkid: 10,
                filters: vec![SubscribeFilter::new("/hello".to_string(), QoS::AtLeastOnce)],
            }),
            Packet::SubAck(SubAck::new(100, vec![SubscribeReasonCode::Failure])),
            Packet::Unsubscribe(Unsubscribe::new("/hello")),
            Packet::UnsubAck(UnsubAck::new(100)),
        ];

        for packet in &packets {
            let mut session = make_session(SessionConfig::new("hello-world"));
            let _connect = session.stream.next().await.unwrap().unwrap();
            session.stream.send(packet).await.unwrap();
            let none = session.stream.next().await;
            assert!(none.is_none());
            assert!(session.join.is_finished());
            let task_res = session.join.await.unwrap().unwrap_err();
            match task_res {
                Error::NotConnack(x) => assert_eq!(x, *packet),
                x => assert!(false, "Unexpected error {:?} for {:?}", x, *packet),
            }
        }
    }

    #[tokio::test]
    async fn disconnect_if_no_connack() {
        tokio::time::pause();

        let mut session = make_session(SessionConfig::new("hello-world"));

        let _connect = session.stream.next().await.unwrap().unwrap();
        tokio::time::advance(Duration::from_secs(5 * 60)).await;
        assert!(!session.join.is_finished());
        tokio::time::advance(Duration::from_secs(151)).await;
        let result = session.join.await.unwrap().unwrap_err();
        assert!(matches!(result, super::Error::KeepAliveTimeout));
    }

    #[tokio::test]
    async fn ping_req_resp() {
        tokio::time::pause();

        let mut session = make_session(SessionConfig::new("hello-world"));
        session.handshake().await;

        for _ in 0..1000 {
            tokio::time::advance(Duration::from_secs(5 * 60 + 1)).await;
            let ping = session.stream.next().await.unwrap().unwrap();
            assert_eq!(ping, Packet::PingReq);
            session.stream.send(v4::PingResp).await.unwrap();
            assert!(!session.join.is_finished());
        }
    }

    #[tokio::test]
    async fn large_keep_alive() {
        tokio::time::pause();

        let mut config = SessionConfig::new("client");
        config.set_keep_alive(Duration::from_secs(0x120012));

        let mut session = make_session(config);
        let connect: Connect = session
            .stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .try_as()
            .unwrap();
        assert_eq!(connect.keep_alive, u16::MAX);
        session
            .stream
            .send(ConnAck {
                code: ConnectReturnCode::Success,
                session_present: false,
            })
            .await
            .unwrap();

        tokio::time::advance(Duration::from_secs(65536)).await;
        let _ping: v4::PingReq = session
            .stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .try_as()
            .unwrap();
    }

    #[tokio::test]
    async fn publish_qos0() {
        let mut session = make_session(SessionConfig::new("client"));
        session.handshake().await;

        let (tx, rx) = tokio::sync::oneshot::channel();
        session
            .task_command
            .send(TaskCommand::Publish(
                Publish::new("/hello", QoS::AtMostOnce, b"This is a test".to_vec()),
                CommandResultTx(tx),
            ))
            .await
            .unwrap();

        let publish: Publish = session
            .stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .try_as()
            .unwrap();
        rx.await.unwrap().unwrap();
        assert_eq!(
            publish,
            Publish::new("/hello", QoS::AtMostOnce, b"This is a test".to_vec())
        );
    }

    #[tokio::test]
    async fn publish_qos1() {
        let mut session = make_session(SessionConfig::new("client"));
        session.handshake().await;

        let (tx, mut rx) = tokio::sync::oneshot::channel();
        session
            .task_command
            .send(TaskCommand::Publish(
                Publish::new("/hello", QoS::AtLeastOnce, b"This is a test".to_vec()),
                CommandResultTx(tx),
            ))
            .await
            .unwrap();

        let publish: Publish = session
            .stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .try_as()
            .unwrap();
        assert_eq!(publish.topic, "/hello");
        assert_eq!(publish.payload, b"This is a test".to_vec());
        assert_eq!(publish.dup, false);
        assert_ne!(publish.pkid, 0);
        assert_eq!(publish.retain, false);
        assert_eq!(publish.qos, QoS::AtLeastOnce);

        for _ in 0..10 {
            rx.try_recv().unwrap_err();
            tokio::task::yield_now().await;
        }

        session
            .stream
            .send(PubAck { pkid: publish.pkid })
            .await
            .unwrap();

        let _ = rx.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn publish_qos1_out_of_order_ack() {
        let mut session = make_session(SessionConfig::new("client"));
        session.handshake().await;

        let mut responses: Vec<_> = (0..10)
            .map(|_| {
                let (tx, rx) = tokio::sync::oneshot::channel();
                session
                    .task_command
                    .try_send(TaskCommand::Publish(
                        Publish::new("/hello", QoS::AtLeastOnce, b"This is a test".to_vec()),
                        CommandResultTx(tx),
                    ))
                    .unwrap();
                rx
            })
            .collect();

        let mut publishes: Vec<Publish> = Vec::new();
        for _ in 0..10 {
            publishes.push(
                session
                    .stream
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .try_as()
                    .unwrap(),
            );
        }

        for _ in 0..10 {
            for rx in &mut responses {
                rx.try_recv().unwrap_err();
                tokio::task::yield_now().await;
            }
        }

        for to_ack in publishes.iter().rev() {
            session
                .stream
                .send(PubAck { pkid: to_ack.pkid })
                .await
                .unwrap();
            let last = responses.pop().unwrap();
            let _ = last.await.unwrap().unwrap();

            for x in &mut responses {
                x.try_recv().unwrap_err();
                tokio::task::yield_now().await;
            }
        }
    }
}
