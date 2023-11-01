use nom::{
    error::{make_error, ErrorKind},
    IResult, Needed, Parser,
};

use super::packet_type::PacketType;

fn make_failure<I>(input: I, err: ErrorKind) -> nom::Err<nom::error::Error<I>> {
    nom::Err::Failure(make_error(input, err))
}

fn make_incomplete<E>(needed: usize) -> nom::Err<nom::error::Error<E>> {
    nom::Err::Incomplete(Needed::new(needed))
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Debug, Clone, Copy, Default)]
#[repr(transparent)]
pub struct VarlenInteger(u32);

impl VarlenInteger {
    pub const MAX: u32 = 268435455;

    pub fn new(val: u32) -> Option<Self> {
        if val <= Self::MAX {
            Some(Self(val))
        } else {
            None
        }
    }

    pub unsafe fn new_unchecked(val: u32) -> Self {
        Self(val)
    }

    pub unsafe fn as_u32(self) -> u32 {
        self.0
    }

    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        if input.is_empty() {
            Err(make_incomplete(1))
        } else {
            let mut value = 0;
            let mut multiplier = 1;
            for x in 0..input.len() {
                value += ((input[x] & 0x7F) as u32) * multiplier;
                multiplier *= 128;
                if input[x] < 128 {
                    return Ok((&input[x + 1..], Self(value)));
                } else if x == 3 {
                    return Err(make_failure(input, ErrorKind::Fail));
                }
            }

            Err(make_incomplete(1))
        }
    }
}

impl PartialEq<u32> for VarlenInteger {
    fn eq(&self, other: &u32) -> bool {
        self.0.eq(other)
    }
}

impl From<VarlenInteger> for u32 {
    fn from(value: VarlenInteger) -> Self {
        value.0
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct FixedHeader {
    pub packet_type: PacketType,
    pub remaining_length: VarlenInteger,
}

impl FixedHeader {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (rest, (packet_type, remaining_length)) =
            (PacketType::parse.and(VarlenInteger::parse)).parse(input)?;
        Ok((
            rest,
            Self {
                packet_type,
                remaining_length,
            },
        ))
    }

    pub fn parse_type(packet: PacketType) -> impl Fn(&[u8]) -> IResult<&[u8], Self> {
        move |input| {
            nom::combinator::verify(Self::parse, |val| {
                val.packet_type == packet
            })(input)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn varlen_int() {
        let zero = VarlenInteger::parse(&[0, 128, 129]).unwrap();
        assert_eq!(zero, ([128, 129].as_slice(), VarlenInteger(0)));

        let max_1byte = VarlenInteger::parse(&[127, 128, 129]).unwrap();
        assert_eq!(max_1byte.1, 127);
        assert_eq!(max_1byte.0, &[128, 129]);

        let two_bytes_min = VarlenInteger::parse(&[128, 1, 129]).unwrap();
        assert_eq!(two_bytes_min.1, 128);
        assert_eq!(two_bytes_min.0, &[129]);

        let two_bytes_max = VarlenInteger::parse(&[255, 127, 129]).unwrap();
        assert_eq!(two_bytes_max.1, 16383);
        assert_eq!(two_bytes_max.0, &[129]);

        let four_bytes_min = VarlenInteger::parse(&[128, 128, 128, 1, 130]).unwrap();
        assert_eq!(four_bytes_min.1, 2097152);
        assert_eq!(four_bytes_min.0, &[130]);

        let four_bytes_max = VarlenInteger::parse(&[255, 255, 255, 127, 131]).unwrap();
        assert_eq!(four_bytes_max.1, VarlenInteger::MAX);
        assert_eq!(four_bytes_max.0, &[131]);

        let incomplete = VarlenInteger::parse(&[255, 255, 255]);
        assert!(matches!(incomplete, Err(nom::Err::Incomplete(_))));

        let incomplete = VarlenInteger::parse(&[]);
        assert!(matches!(incomplete, Err(nom::Err::Incomplete(_))));

        let incomplete = VarlenInteger::parse(&[255, 255, 255, 255]);
        assert!(matches!(incomplete, Err(nom::Err::Failure(_))));
    }
}
