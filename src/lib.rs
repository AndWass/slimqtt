//! A small, opinionated, scaled down MQTT client library. It strives to expose a small set of features
//! but still be easy to use.
//!
//! # Building blocks
//!
//! The fundamental building block used by the library is the [`Session`] type. A session uses
//! an already-connected transport to communicate with a broker, ending when an error is encountered
//! or the user wants to shut down the session.
//!
//! A session can be reset, effectively reconnecting it to the broker again, but this must always
//! be done from outside the session.
//!
//! [`Session`]: crate::session::Session
//!
//! ## Examples
//!
//! TODO
pub mod session;

mod codec;
