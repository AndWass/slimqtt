use crate::session::task::CommandResultTx;
use crate::session::PublishError;

pub enum WaitingAck {
    PubAck(CommandResultTx<(), PublishError>),
}

impl WaitingAck {
    pub fn set_puback_result(self, res: Result<(), PublishError>) {
        match self {
            WaitingAck::PubAck(ch) => {
                let _ = ch.0.send(res);
            }
        }
    }
}

pub struct WaitAckSlots {
    slab: slab::Slab<WaitingAck>,
}

impl WaitAckSlots {
    pub fn new(capacity: usize) -> Self {
        Self {
            slab: slab::Slab::with_capacity(capacity),
        }
    }

    pub fn try_insert(&mut self, ack: WaitingAck) -> Result<u16, WaitingAck> {
        let entry = self.slab.vacant_entry();
        let key = entry.key();

        if key < 65535 {
            entry.insert(ack);
            Ok((key + 1) as u16)
        } else {
            Err(ack)
        }
    }

    pub fn take_puback(&mut self, packet_id: u16) -> Option<WaitingAck> {
        if packet_id == 0 {
            None
        } else {
            let key: usize = (packet_id - 1).into();
            match self.slab.get(key)? {
                WaitingAck::PubAck(_) => Some(self.slab.remove(key)),
            }
        }
    }
}
