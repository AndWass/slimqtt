use std::time::Duration;
use tokio_util::either::Either;

pub enum KeepAlive {
    PingRequest,
    PingResponseDeadline,
}

pub struct KeepAliveTimer {
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