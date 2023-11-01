/*use super::{Encodable, fixed_header::{FixedHeader, VarlenInteger}};

pub struct Connect {
    pub clean_session: bool,
}

impl Connect {
    fn encode_body<F: FnMut(&[u8]) -> bool>(&self, mut encoder: F) -> bool {
        const MQTT4: [u8;5] = [b'M', b'Q', b'T', b'T', 4];

        (encoder)(&MQTT4)

    }

    fn size_body(&self) -> Option<usize> {
        let mut res = 0;
        self.encode_body(|x| {
            res += x.len();
            true
        }).then(|| res)
    }
}

impl Encodable for Connect {
    fn encode<F: FnMut(&[u8]) -> bool>(&self, mut encoder: F) -> bool {
        if let Some(size) = self.size_body() {
            let header = FixedHeader {
                first_byte: 0x10,
                remaining_length: VarlenInteger(size as u32)
            };
            header.encode(&mut encoder) && self.encode_body(encoder)
        }
        else {
            false
        }
    }
}*/