use bytes::Bytes;
use futures::{Sink, SinkExt, TryStreamExt};
use mqttbytes::v4::Packet;
use mqttbytes::QoS;
use std::time::Duration;

use crate::session::{Error, SessionConfig};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::codec::CodecError;
use tokio_util::either::Either;

enum KeepAlive {
    PingRequest,
    PingResponseDeadline,
}

struct KeepAliveTimer {
    timer: Either<(tokio::time::Interval, tokio::time::Interval), ()>,
}

impl KeepAliveTimer {
    pub fn new(duration: Duration) -> Self {
        let timer = if duration.is_zero() {
            Either::Right(())
        } else {
            Either::Left((
                tokio::time::interval(duration),
                tokio::time::interval(duration + duration / 2),
            ))
        };
        Self { timer }
    }

    pub fn reset(&mut self) {
        match &mut self.timer {
            Either::Left((ping, deadline)) => {
                ping.reset();
                deadline.reset();
            }
            Either::Right(_) => {}
        }
    }
    pub async fn wait(&mut self) -> KeepAlive {
        match &mut self.timer {
            Either::Left((ping, deadline)) => Self::tick(ping, deadline).await,
            Either::Right(_) => futures::future::pending().await,
        }
    }

    async fn tick(
        ping: &mut tokio::time::Interval,
        deadline: &mut tokio::time::Interval,
    ) -> KeepAlive {
        tokio::select! {
            _ = ping.tick() => {
                KeepAlive::PingRequest
            },
            _ = deadline.tick() => {
                KeepAlive::PingResponseDeadline
            }
        }
    }
}

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
pub struct PublishEvent {
    pub topic: String,
    pub data: Bytes,
    pub response: tokio::sync::oneshot::Sender<Result<(), crate::session::PublishError>>,
}

#[derive(Debug)]
pub(crate) enum SessionEvent {
    Publish(PublishEvent),
}

pub struct SessionTask<T> {
    stream: MqttStream<T>,
    state: State,
    keep_alive: KeepAliveTimer,
    config: SessionConfig,
    session_events: tokio::sync::mpsc::Receiver<SessionEvent>,
}

impl<T: Unpin + AsyncRead + AsyncWrite> SessionTask<T> {
    pub(crate) fn new(
        stream: T,
        config: SessionConfig,
    ) -> (Self, tokio::sync::mpsc::Sender<SessionEvent>) {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        (
            Self {
                stream: MqttStream::new(stream),
                state: State::SendConnect,
                keep_alive: KeepAliveTimer::new(config.keep_alive()),
                config,
                session_events: rx,
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

    async fn handle_connected_session_event(&mut self, event: SessionEvent) -> Result<(), Error> {
        use mqttbytes::v4;
        match event {
            SessionEvent::Publish(evt) => {
                log::debug!("Publishing {} bytes to {}", evt.data.len(), evt.topic);
                let result = self
                    .stream
                    .send(v4::Publish::new(evt.topic, QoS::AtMostOnce, evt.data))
                    .await;
                let response = if let Err(e) = &result {
                    Err(crate::session::PublishError::from(e))
                } else {
                    Ok(())
                };
                let _ = evt.response.send(response);
                result?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::codec::TryAs;
    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};
    use mqttbytes::v4::Packet;
    use mqttbytes::{v4, QoS};
    use tokio::io::DuplexStream;
    use tokio::task::JoinHandle;

    use crate::session::{Error, SessionConfig};

    use super::SessionTask;

    fn make_session(
        config: SessionConfig,
    ) -> (
        crate::codec::Framed<DuplexStream>,
        JoinHandle<Result<(), Error>>,
    ) {
        let (s, stream) = tokio::io::duplex(256);
        let (mut task, evt_tx) = SessionTask::new(s, config);
        let join = tokio::spawn(async move { task.run().await });

        (crate::codec::Framed::new(stream), join)
    }

    #[tokio::test]
    async fn first_message_is_connect() {
        let (mut stream, _join) = make_session(SessionConfig::new("hello-world"));

        let message = stream.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            Packet::Connect(v4::Connect {
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
            let (mut stream, task) = make_session(SessionConfig::new("hello-world"));
            let _connect = stream.next().await.unwrap().unwrap();
            stream.send(packet).await.unwrap();
            let none = stream.next().await;
            assert!(none.is_none());
            assert!(task.is_finished());
            let task_res = task.await.unwrap().unwrap_err();
            match task_res {
                Error::NotConnack(x) => assert_eq!(x, *packet),
                x => assert!(false, "Unexpected error {:?} for {:?}", x, *packet),
            }
        }
    }

    #[tokio::test]
    async fn disconnect_if_no_connack() {
        tokio::time::pause();

        let (mut stream, task) = make_session(SessionConfig::new("hello-world"));

        let _connect = stream.next().await.unwrap().unwrap();
        tokio::time::advance(Duration::from_secs(5 * 60)).await;
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_secs(151)).await;
        let result = task.await.unwrap().unwrap_err();
        assert!(matches!(result, super::Error::KeepAliveTimeout));
    }

    #[tokio::test]
    async fn ping_req_resp() {
        tokio::time::pause();

        let (mut stream, task) = make_session(SessionConfig::new("hello-world"));

        let _connect = stream.next().await.unwrap().unwrap();
        stream
            .send(v4::ConnAck {
                code: v4::ConnectReturnCode::Success,
                session_present: false,
            })
            .await
            .unwrap();

        for _ in 0..1000 {
            tokio::time::advance(Duration::from_secs(5 * 60 + 1)).await;
            let ping = stream.next().await.unwrap().unwrap();
            assert_eq!(ping, Packet::PingReq);
            stream.send(v4::PingResp).await.unwrap();
            assert!(!task.is_finished());
        }
    }

    #[tokio::test]
    async fn large_keep_alive() {
        tokio::time::pause();

        let mut config = SessionConfig::new("client");
        config.set_keep_alive(Duration::from_secs(0x120012));

        let (mut stream, _) = make_session(config);
        let connect: v4::Connect = stream.next().await.unwrap().unwrap().try_as().unwrap();
        assert_eq!(connect.keep_alive, u16::MAX);
        stream
            .send(v4::ConnAck {
                code: v4::ConnectReturnCode::Success,
                session_present: false,
            })
            .await
            .unwrap();

        tokio::time::advance(Duration::from_secs(65536)).await;
        let _ping: v4::PingReq = stream.next().await.unwrap().unwrap().try_as().unwrap();
    }
}
