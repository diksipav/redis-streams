#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("stream not found")]
    StreamNotFound,

    #[error("invalid id format, should be <milliseconds>-<sequence>")]
    BadIdFormat,

    #[error("id must be greater than last id")]
    IdNotMonotonic,

    #[error("id must have timestamp greater than 0")]
    IdShoulHavePositiveTimestamp,

    #[error("timeout")]
    Timeout,

    #[error("invalid arguments")]
    InvalidArgs,

    #[error("field count must be even")]
    InvalidFieldCount,
}

pub type Result<T> = std::result::Result<T, Error>;
