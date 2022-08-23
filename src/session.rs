//! An async communication session with a broker.
//!
//! A session is started by establishing a TCP/TLS/WebSocket/other connection with a broker.
//! Any stream that implements [`AsyncRead`] and [`AsyncWrite`] can be used ([`AsyncRead`] must be
//! cancellation safe!). The stream is then used to create a [`SessionTask`] object that will
//! perform the necessary MQTT handshakes and communicate with the broker. The user communicates
//! with the session task via the [`Session`] object. It is also the users responsibility to ensure
//! that the session task is run.
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
//! # use slimqtt::session::SessionTask;
//! # use std::time::Duration;
//! # use slimqtt::session::{Session, SessionConfig};
//! # use tokio::net::TcpStream;
//! # tokio_test::block_on( async move {
//! let mut options = SessionConfig::new("slimqtt-client");
//! options.set_keep_alive(Duration::from_secs(5));
//!
//! let stream = TcpStream::connect("test.mosquitto.org:1883").await.unwrap();
//! let (mut session, mut task) = Session::new(stream, options);
//! // Run the task loop. If/when this returns
//! // the session cannot be restarted without calling reset first.
//! println!("Session result: {:?}", task.run().await);
//! # });
//! ```

mod task;
mod keep_alive;

use std::time::Duration;

use mqttbytes::v4::Packet;

use crate::codec::CodecError;

use mqttbytes::v4;
pub use mqttbytes::v4::{LastWill, Login, SubscribeFilter, Publish};
pub use mqttbytes::QoS;
pub use task::SessionTask;
use tokio::io::{AsyncRead, AsyncWrite};

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
    #[error("The user requested that the session should be closed")]
    UserStop,
}

impl From<CodecError> for Error {
    fn from(v: CodecError) -> Self {
        match v {
            CodecError::IoError(io) => io.into(),
            CodecError::ProtocolError(e) => Self::ProtocolError(e),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("Some IO error ocurred: {0}")]
    IoError(std::io::ErrorKind),
    #[error("Protocol message error: {0}")]
    ProtocolError(mqttbytes::Error),
    #[error("Session has ended before message could be published")]
    SessionEnded,
    #[error("Unspecified error: {0}")]
    Unspecified(String),
}

impl From<&Error> for PublishError {
    fn from(err: &Error) -> Self {
        match err {
            Error::IoError(io) => Self::IoError(io.kind()),
            Error::ProtocolError(x) => Self::ProtocolError(x.clone()),
            x => Self::Unspecified(format!("{:?}", x))
        }
    }
}

/// Configuration values for setting up a session
#[derive(Clone, PartialEq, Debug)]
pub struct SessionConfig {
    connect: v4::Connect,
}

impl SessionConfig {
    /// Create a new [`SessionConfig`].
    ///
    /// The following default values are used:
    ///
    /// * `login`: `None`
    /// * `keep_alive`: 5 minutes
    /// * `clean_session`: `false`
    /// * `last_will`: `None`
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
    ///
    /// assert_eq!(config, SessionConfig::new("slimqtt-client")
    /// .set_login(None)
    /// .set_clean_session(false)
    /// .set_keep_alive(Duration::from_secs(5*60))
    /// .clone());
    /// ```
    pub fn new<S: ToString>(client_id: S) -> Self {
        SessionConfig {
            connect: v4::Connect {
                client_id: client_id.to_string(),
                login: None,
                keep_alive: 5 * 60,
                clean_session: false,
                last_will: None,
                protocol: mqttbytes::Protocol::V4,
            },
        }
    }

    /// Get the login data of the session configuration
    pub fn login(&self) -> &Option<Login> {
        &self.connect.login
    }

    /// Set new login data
    pub fn set_login(&mut self, login: Option<Login>) -> &mut Self {
        self.connect.login = login;
        self
    }

    /// Get the current keep alive configuration.
    pub fn keep_alive(&self) -> Duration {
        Duration::from_secs(self.connect.keep_alive.into())
    }

    /// Set keep alive. Note that MQTT only uses whole seconds for keep alive.
    ///
    /// A value greater than `u16::MAX` seconds will be truncated to `u16::MAX`.
    ///
    /// Set to zero to disable keep alive handling (not recommended though).
    pub fn set_keep_alive(&mut self, keep_alive: Duration) -> &mut Self {
        self.connect.keep_alive = keep_alive
            .min(Duration::from_secs(u16::MAX.into()))
            .as_secs() as u16;
        self
    }

    /// Get the current clean session configuration.
    pub fn clean_session(&self) -> bool {
        self.connect.clean_session
    }

    /// Set a new clean session configuration
    pub fn set_clean_session(&mut self, clean_session: bool) -> &mut Self {
        self.connect.clean_session = clean_session;
        self
    }

    /// Get the configured last will
    pub fn last_will(&self) -> &Option<LastWill> {
        &self.connect.last_will
    }

    /// Set a new last will to be used.
    pub fn set_last_will(&mut self, last_will: Option<LastWill>) -> &mut Self {
        self.connect.last_will = last_will;
        self
    }

    pub(crate) fn as_connect(&self) -> &v4::Connect {
        &self.connect
    }
}

pub struct Session {
    task_channel: tokio::sync::mpsc::Sender<task::TaskCommand>,
}

impl Session {
    pub fn new<Stream>(stream: Stream, config: SessionConfig) -> (Session, SessionTask<Stream>)
    where
        Stream: Unpin + AsyncRead + AsyncWrite,
    {
        let (task, channel) = SessionTask::new(stream, config);
        (
            Session {
                task_channel: channel,
            },
            task,
        )
    }

    pub async fn publish(
        &mut self,
        publish: v4::Publish
    ) -> Result<(), PublishError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.task_channel
            .send(task::TaskCommand::Publish(publish, tx))
            .await
            .or_else(|_| Err(PublishError::SessionEnded))?;

        rx.await.unwrap_or(Err(PublishError::SessionEnded))
    }
}
