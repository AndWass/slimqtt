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
//! tokio_test::block_on( async move {
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

mod task;

use std::time::Duration;

use mqttbytes::v4::Packet;

use crate::codec::CodecError;

pub use mqttbytes::v4::{Login, SubscribeFilter};
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
}

impl From<CodecError> for Error {
    fn from(v: CodecError) -> Self {
        match v {
            CodecError::IoError(io) => io.into(),
            CodecError::ProtocolError(e) => Self::ProtocolError(e),
        }
    }
}

/// Configuration values for setting up a session
#[derive(Clone, PartialEq, Debug)]
pub struct SessionConfig {
    /// Client ID sent to the broker.
    pub client_id: String,
    /// Optional login data
    pub login: Option<Login>,
    /// Keep alive. Note that MQTT only uses whole seconds for keep alive.
    ///
    /// A value greater than `u16::MAX` seconds will be truncated to `u16::MAX`.
    ///
    /// Set to zero to disable keep alive handling (not recommended though). See the documentation
    /// for [`SessionTask`] for more information on how keep alive is used.
    ///
    ///
    pub keep_alive: Duration,
    /// Request a clean session or to reuse an existing session on the broker side.
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

pub struct Session {}

impl Session {
    pub fn new<Stream>(stream: Stream, config: SessionConfig) -> SessionTask<Stream>
    where
        Stream: Unpin + AsyncRead + AsyncWrite,
    {
        SessionTask::new(stream, config)
    }
}
