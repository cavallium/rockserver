use super::io::SafeDataInput;
use std::io;

pub trait Decodable: Sized {
    fn decode<R: SafeDataInput + ?Sized>(input: &mut R) -> io::Result<Self>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Int52(pub i64);

impl Decodable for Int52 {
    fn decode<R: SafeDataInput + ?Sized>(input: &mut R) -> io::Result<Self> {
        input.read_int52().map(Int52)
    }
}
