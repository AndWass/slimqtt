use nom::{IResult, Parser, Needed, error::{make_error, ErrorKind}};

fn match_exact(byte: u8) -> impl Fn(&[u8]) -> IResult<&[u8], u8> {
    move |input| {
        if input.is_empty() {
            Err(nom::Err::Incomplete(Needed::new(1)))
        }
        else if input[0] != byte {
            Err(nom::Err::Error(make_error(input, ErrorKind::Fail)))
        }
        else {
            Ok((&input[1..], byte))
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum PacketType {
    Connect,
    ConnAck,
}

impl PacketType {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let connect = match_exact(0x10).map(|_| PacketType::Connect);
        let connack = match_exact(0x20).map(|_| PacketType::ConnAck);

        connect.or(connack).parse(input)
    }
}