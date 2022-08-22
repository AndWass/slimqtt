//! An async communication session with a broker.
//!
//! A session is started by establishing a TCP/TLS/WebSocket/other connection with a broker.
//! Any stream that implements [`AsyncRead`] and [`AsyncWrite`] can be used ([`AsyncRead`] must be
//! cancellation safe!). The stream is then used to create a [`Session`] object that will
//! perform the necessary MQTT handshakes and communicate with the broker.
//!
//! [`AsyncRead`]: https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html
//! [`AsyncWrite`]: https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html
//!
//! ## Session states
//!
//! ### Initial setup
//!
//! Each session will perform the following actions before it is considered established:
//!
//!   1. Send [`CONNECT`](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718028)
//!   2. Wait for [`CONNACK`](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718033)
//!
//! **No ping requests are sent while waiting for `CONNACK`.
//!
//! After `CONNACK` has been received the session is considered established and messages can be published
//! and received.
//!
//! ### Operation of established sessions
//!
//! An established session will automatically send [`PINGREQ`](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718081)
//! if a non-zero keep alive has been set.
//!
//! It will also handle responding to publishes from the broker.
//!
//! #### QoS 2 handling
//!
//! The session will automatically respond to QoS 2 messages with [`PUBREC`](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718048)
//! but it will otherwise treat the message as if it was sent with QoS 1.
//!
//! [`PUBREL`](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718053) will always cause a corresponding
//! [`PUBCOMP`](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718058) to be sent
//! but no other action is taken.
//!
//! **QoS 2** in outgoing publishes are not supported and will result in an error rather than anything being written.
//!
//! ## Examples
//!
//! ### Session creation
//!
//! ```no_run
//! # tokio_test::block_on( async move {
//! use std::time::Duration;
//! use slimqtt::session::{Session, SessionConfig};
//! use tokio::net::TcpStream;
//!
//! let mut options = SessionConfig::new("slimqtt-client");
//! options.keep_alive = Duration::from_secs(5);
//!
//! let stream = TcpStream::connect("test.mosquitto.org:1883").await.unwrap();
//! let mut session = Session::new(stream, options);
//! // Run the session event loop. If/when this returns
//! // the session cannot be restarted without calling reset first.
//! println!("Session result: {:?}", session.run().await);
//! # });
//! ```

use futures::{SinkExt, TryStreamExt};
use mqttbytes::v4::Packet;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Encoder;
use tokio_util::either::Either;

use crate::codec::{Codec, CodecError};

pub use mqttbytes::v4::{Login, SubscribeFilter};
pub use mqttbytes::QoS;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("MQTT protocol error: {0}")]
    ProtocolError(mqttbytes::Error),
    #[error("MQTT Connect rejected: {0:?}")]
    ConnectionRejected(mqttbytes::v4::ConnectReturnCode),
    #[error("CONNACK expected, got {0:?}")]
    NotConnack(Packet),
    #[error("Keep alive timeout")]
    KeepAliveTimeout,
    #[error("The session must be reset before it can be used again")]
    NeedReset,
    #[error("Connection closed for unknown reason")]
    ConnectionClosed,
}

impl From<CodecError> for Error {
    fn from(v: CodecError) -> Self {
        match v {
            CodecError::IoError(io) => io.into(),
            CodecError::ProtocolError(e) => Self::ProtocolError(e),
        }
    }
}

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
        loop {
            let packet = self.stream.try_next().await?;
            return if let Some(packet) = packet {
                Ok(packet)
            } else {
                log::warn!("Unexpected None from stream.try_next");
                Err(Error::ConnectionClosed)
            };
        }
    }

    pub async fn send<Item>(&mut self, item: Item) -> Result<(), Error>
    where
        Codec: Encoder<Item>,
        Error: From<<Codec as Encoder<Item>>::Error>,
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

/// Configuration values for setting up a session
#[derive(Clone, PartialEq, Debug)]
pub struct SessionConfig {
    pub client_id: String,
    pub login: Option<Login>,
    pub keep_alive: Duration,
    pub clean_session: bool,
}

impl SessionConfig {
    /// Create a new [`SessionConfig`].
    ///
    /// The following default values are used:
    ///
    /// * `login`: `None`
    /// * `keep_alive`: 5 minutes
    /// * `clean_session`: `false`
    ///
    /// # Arguments
    ///
    /// * `client_id`: The client ID to use in the `CONNACK` message.
    ///
    /// `client_id` is not verified in any way to be conforming to the MQTT specification.
    ///
    /// returns: `SessionConfig`
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use slimqtt::session::SessionConfig;
    /// let config = SessionConfig::new("slimqtt-client");
    /// assert_eq!(config, SessionConfig {
    ///     client_id: "slimqtt-client".to_string(),
    ///     login: None,
    ///     keep_alive: Duration::from_secs(5*60),
    ///     clean_session: false,
    /// });
    /// ```
    pub fn new<S: ToString>(client_id: S) -> Self {
        SessionConfig {
            client_id: client_id.to_string(),
            login: None,
            keep_alive: Duration::from_secs(5 * 60),
            clean_session: false,
        }
    }
}

pub struct Session<T> {
    stream: MqttStream<T>,
    state: State,
    keep_alive: KeepAliveTimer,
    connect: mqttbytes::v4::Connect,
}

impl<T: Unpin + AsyncRead + AsyncWrite> Session<T> {
    pub fn new(stream: T, config: SessionConfig) -> Self {
        let mut connect = mqttbytes::v4::Connect::new(config.client_id);
        connect.clean_session = config.clean_session;
        connect.keep_alive = config.keep_alive.as_secs() as u16;
        connect.login = config.login;

        Self {
            stream: MqttStream::new(stream),
            state: State::SendConnect,
            keep_alive: KeepAliveTimer::new(config.keep_alive),
            connect,
        }
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
                    self.stream.send(&self.connect).await?;
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
        use mqttbytes::v4::*;
        loop {
            tokio::select! {
                packet = self.stream.next() => {
                    self.keep_alive.reset();
                    match packet? {
                        Packet::Publish(publish) => {
                            log::debug!("Received {:?}", publish);
                            if publish.qos == QoS::AtLeastOnce {
                                log::debug!("Sending PUBACK for {}", publish.pkid);
                                self.stream.send(&PubAck::new(publish.pkid)).await?;
                            }
                            else if publish.qos == QoS::ExactlyOnce {
                                log::debug!("Sending PUBREC for {}", publish.pkid);
                                self.stream.send(&PubRec::new(publish.pkid)).await?;
                            }
                        },
                        Packet::PubRel(pubrel) => {
                            log::debug!("PUBREL received: {:?}", pubrel);
                            self.stream.send(&PubComp::new(pubrel.pkid)).await?;
                        },
                        Packet::Disconnect => {
                            log::debug!("DISCONNECT received");
                            return Err(Error::ConnectionClosed);
                        }
                        x => {
                            log::debug!("Received {:?}", x);
                        }
                    }
                },
                keep_alive = self.keep_alive.wait() => {
                    match keep_alive {
                        KeepAlive::PingRequest => {
                            log::debug!("Writing ping request");
                            self.stream.send(&mqttbytes::v4::PingReq).await?;
                        },
                        KeepAlive::PingResponseDeadline => {
                            log::debug!("Ping response deadline reached");
                            return Err(Error::KeepAliveTimeout);
                        }
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::Codec;
    use crate::session::{Session, SessionConfig};
    use futures::StreamExt;
    use mqttbytes::v4;
    use mqttbytes::v4::Packet;
    use std::time::Duration;

    struct Fixture {

    }

    #[tokio::test]
    async fn first_message_is_connect() {
        let (s, stream) = tokio::io::duplex(256);
        tokio::spawn(async move {
            Session::new(s, SessionConfig::new("hello-world"))
                .run()
                .await
        });

        let mut stream = tokio_util::codec::Framed::new(stream, Codec);
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
    async fn disconnect_if_no_connack() {
        tokio::time::pause();

        let (s, stream) = tokio::io::duplex(256);
        let task = tokio::spawn(async move {
            Session::new(s, SessionConfig::new("hello-world"))
                .run()
                .await
        });

        let mut stream = tokio_util::codec::Framed::new(stream, Codec);
        let _connack = stream.next().await.unwrap().unwrap();
        tokio::time::advance(Duration::from_secs(5 * 60)).await;
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_secs(151)).await;
        let result = task.await.unwrap().unwrap_err();
        assert!(matches!(result, super::Error::KeepAliveTimeout));
    }
}
