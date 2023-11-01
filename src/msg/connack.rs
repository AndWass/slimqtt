use nom::{error::{make_error, ErrorKind}, IResult, Needed};

use super::{fixed_header::FixedHeader, packet_type::PacketType};

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ConnAckCode {
    Accepted,
    UnacceptableProtocol,
    RejectedIdentifier,
    ServerUnavailable,
    BadAuthentication,
    Unauthorized,
    Reserved(u8)
}

impl From<u8> for ConnAckCode {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Accepted,
            1 => Self::UnacceptableProtocol,
            2 => Self::RejectedIdentifier,
            3 => Self::ServerUnavailable,
            4 => Self::BadAuthentication,
            6 => Self::Unauthorized,
            x => Self::Reserved(x)
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct ConnAck {
    pub session_present: bool,
    pub code: ConnAckCode,
}

impl ConnAck {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (remaining, fixed_header) = FixedHeader::parse_type(PacketType::ConnAck)(input)?;
        if fixed_header.remaining_length == 2 {
            if remaining.len() < 2 {
                return Err(nom::Err::Incomplete(Needed::new(2 - remaining.len())));
            }
            if remaining[0] > 1 {
                return Err(nom::Err::Failure(make_error(input, ErrorKind::Verify)));
            }

            let session_present = remaining[0] == 1;
            let return_code = remaining[1];
            Ok((&remaining[2..], Self {session_present, code: return_code.into() }))
        }
        else {
            Err(nom::Err::Failure(make_error(input, ErrorKind::Fail)))
        }
    }
}

#[cfg(test)]
mod tests {
    use nom::Parser;

    use crate::msg::connack::ConnAckCode;

    use super::ConnAck;

    #[test]
    fn parse() {
        let data = [0x20, 2, 0, 0, 0x10];
        let (rest, data) = ConnAck::parse.parse(&data).unwrap();
        assert_eq!(rest, &[0x10]);
        assert_eq!(data.session_present, false);
        assert_eq!(data.code, ConnAckCode::Accepted);

        let data = [0x10, 0x00];
        let res = ConnAck::parse.parse(&data).unwrap_err();
        assert!(matches!(res, nom::Err::Error(_)));
    }
}