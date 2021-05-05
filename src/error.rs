use std::fmt;

pub type Res<T> = Result<T, Box<dyn std::error::Error>>;

#[macro_export]
macro_rules! err {
    ( $( $a:expr) , + ) => {
        crate::error::Error::boxed(format!( $( $a, )* ).as_str())
    };
}

#[derive(Debug)]
pub struct Error {
    details: String,
}

impl Error {
    pub fn new(msg: &str) -> Error {
        Error {
            details: msg.to_string(),
        }
    }

    pub fn boxed(msg: &str) -> Box<Error> {
        Box::new(Self::new(msg))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        &self.details
    }
}
