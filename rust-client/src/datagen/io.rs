use std::io::{self, Read};

/// Trait for reading Cavallium Data Generator serialized formats.
pub trait SafeDataInput {
    fn read_boolean(&mut self) -> io::Result<bool>;
    fn read_byte(&mut self) -> io::Result<i8>;
    fn read_unsigned_byte(&mut self) -> io::Result<u8>;
    fn read_short(&mut self) -> io::Result<i16>;
    fn read_unsigned_short(&mut self) -> io::Result<u16>;
    fn read_char(&mut self) -> io::Result<char>;
    fn read_int(&mut self) -> io::Result<i32>;
    fn read_long(&mut self) -> io::Result<i64>;
    fn read_int52(&mut self) -> io::Result<i64>;
    fn read_float(&mut self) -> io::Result<f32>;
    fn read_double(&mut self) -> io::Result<f64>;
    fn read_short_text(&mut self) -> io::Result<String>;
    fn read_medium_text(&mut self) -> io::Result<String>;
    fn read_bytes(&mut self, len: usize) -> io::Result<Vec<u8>>;
    fn read_bytes_with_len(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_int()?;
        if len < 0 || len > 100 * 1024 * 1024 { // 100 MB limit
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Byte array too large"));
        }
        self.read_bytes(len as usize)
    }
}

impl<R: Read> SafeDataInput for R {
    fn read_boolean(&mut self) -> io::Result<bool> {
        let val = self.read_unsigned_byte()?;
        Ok(val != 0)
    }

    fn read_byte(&mut self) -> io::Result<i8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0] as i8)
    }

    fn read_unsigned_byte(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_short(&mut self) -> io::Result<i16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(i16::from_be_bytes(buf))
    }

    fn read_unsigned_short(&mut self) -> io::Result<u16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(u16::from_be_bytes(buf))
    }

    fn read_char(&mut self) -> io::Result<char> {
        let val = self.read_unsigned_short()?;
        // Java writes char as 2 bytes (UTF-16 unit), but here we might just cast.
        // Rust char is 4 bytes. If it's a standard ASCII/BMP char, this works.
        // For correct Java char decoding, we might need to be careful, but often it's just cast.
        // However, char::from_u32 is safer.
        char::from_u32(val as u32).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid char"))
    }

    fn read_int(&mut self) -> io::Result<i32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    fn read_long(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }

    fn read_int52(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 7];
        self.read_exact(&mut buf)?;
        
        let val = ((buf[0] as i64 & 0x0F) << 48)
                | ((buf[1] as i64 & 0xFF) << 40)
                | ((buf[2] as i64 & 0xFF) << 32)
                | ((buf[3] as i64 & 0xFF) << 24)
                | ((buf[4] as i64 & 0xFF) << 16)
                | ((buf[5] as i64 & 0xFF) << 8)
                | (buf[6] as i64 & 0xFF);
        
        Ok(val)
    }

    fn read_float(&mut self) -> io::Result<f32> {
        let val = self.read_int()?;
        Ok(f32::from_bits(val as u32))
    }

    fn read_double(&mut self) -> io::Result<f64> {
        let val = self.read_long()?;
        Ok(f64::from_bits(val as u64))
    }

    fn read_short_text(&mut self) -> io::Result<String> {
        let len = self.read_unsigned_short()? as usize;
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn read_medium_text(&mut self) -> io::Result<String> {
        let len = self.read_int()? as usize;
        if len > 10 * 1024 * 1024 { // 10 MB limit
            return Err(io::Error::new(io::ErrorKind::InvalidData, "String too large"));
        }
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn read_bytes(&mut self, len: usize) -> io::Result<Vec<u8>> {
        if len > 100 * 1024 * 1024 { // 100 MB limit
             return Err(io::Error::new(io::ErrorKind::InvalidData, "Byte array too large"));
        }
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }
}
