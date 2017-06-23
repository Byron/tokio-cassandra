use std::io;
use std::error;

pub fn io_err<S>(msg: S) -> io::Error
where
    S: Into<Box<error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, msg)
}
