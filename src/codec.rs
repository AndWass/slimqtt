use bytes::BytesMut;
use futures::{Sink, Stream, StreamExt};
use mqttbytes::v4::Packet;
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder};

pub const MAX_PACKET_SIZE: usize = 256 * 1024;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("MQTT protocol error {0}")]
    ProtocolError(mqttbytes::Error),
}

impl From<mqttbytes::Error> for CodecError {
    fn from(v: mqttbytes::Error) -> Self {
        Self::ProtocolError(v)
    }
}

pub struct Codec;

impl Decoder for Codec {
    type Item = Packet;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match mqttbytes::check(src.iter(), MAX_PACKET_SIZE) {
            Ok(_header) => {
                let res = mqttbytes::v4::read(src, MAX_PACKET_SIZE)?;
                Ok(Some(res))
            }
            Err(mqttbytes::Error::InsufficientBytes(x)) => {
                if src.capacity() < x {
                    src.reserve(x - src.capacity());
                }
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl Encoder<&Packet> for Codec {
    type Error = CodecError;

    fn encode(&mut self, item: &Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Packet::Connect(p) => p.write(dst)?,
            Packet::ConnAck(p) => p.write(dst)?,
            Packet::Publish(p) => p.write(dst)?,
            Packet::PubAck(p) => p.write(dst)?,
            Packet::PubRec(p) => p.write(dst)?,
            Packet::PubRel(p) => p.write(dst)?,
            Packet::PubComp(p) => p.write(dst)?,
            Packet::Subscribe(p) => p.write(dst)?,
            Packet::SubAck(p) => p.write(dst)?,
            Packet::Unsubscribe(p) => p.write(dst)?,
            Packet::UnsubAck(p) => p.write(dst)?,
            Packet::PingReq => mqttbytes::v4::PingReq.write(dst)?,
            Packet::PingResp => mqttbytes::v4::PingResp.write(dst)?,
            Packet::Disconnect => mqttbytes::v4::Disconnect.write(dst)?,
        };

        Ok(())
    }
}

macro_rules! enc_impl {
    ($for_:ident) => {
        impl Encoder<&mqttbytes::v4::$for_> for Codec {
            type Error = CodecError;
            fn encode(
                &mut self,
                item: &mqttbytes::v4::$for_,
                dst: &mut BytesMut,
            ) -> Result<(), Self::Error> {
                item.write(dst)?;
                Ok(())
            }
        }
    };
}

enc_impl!(ConnAck);
enc_impl!(Connect);
enc_impl!(Disconnect);
enc_impl!(PingReq);
enc_impl!(PingResp);
enc_impl!(PubAck);
enc_impl!(PubComp);
enc_impl!(PubRec);
enc_impl!(PubRel);
enc_impl!(Publish);
enc_impl!(SubAck);
enc_impl!(Subscribe);
enc_impl!(UnsubAck);
enc_impl!(Unsubscribe);

use pin_project_lite::pin_project;

pin_project! {
pub struct Framed<T> {
    #[pin]
    inner: tokio_util::codec::Framed<T, Codec>
}
}

impl<T: Unpin + AsyncRead + AsyncWrite> Framed<T> {
    pub fn new(stream: T) -> Self {
        Self {
            inner: tokio_util::codec::Framed::new(stream, Codec),
        }
    }
}

impl<T> Stream for Framed<T>
where
    tokio_util::codec::Framed<T, Codec>: Stream,
{
    type Item = <tokio_util::codec::Framed<T, Codec> as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}

impl<T: AsyncWrite, I> Sink<I> for Framed<T>
where
    Codec: Encoder<I>,
    tokio_util::codec::Framed<T, Codec>: Sink<I>,
{
    type Error = <tokio_util::codec::Framed<T, Codec> as Sink<I>>::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        this.inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let this = self.project();
        this.inner.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        this.inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        this.inner.poll_close(cx)
    }
}
