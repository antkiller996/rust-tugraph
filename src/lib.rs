// Copyright 2023 antkiller
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # Know issues
//! ## Memory leak
//! TuGraph use a smart pointer type([`GCRefCountedPtr<T>`]) and its wingman scoped reference([`ScopedRef<T>`]) to
//! manage long-living objects which have few construct/destruct and a lot of references. If the last `GCRefCountedPtr` is
//! destructed but with some ScopedRefs alive, the underlying T* deallocation task will be sent into [`TimedTaskScheduler`].
//! If `TimedTaskScheduler` runs long enough, the underlying T* will be deallocated(GC). However, `TimedTaskScheduler`
//! is a static singleton mananger, its [`destructor`] will be called when the program is going to shutdown, make some underlying T*
//! deallocation task cancelled and leave some T* leak. It is the root cause to memory leak.
//!
//! What's worse, there is no api to wait until all tasks in `TimedTaskScheduler` to be finished in order to make sure all T* are deallocated.
//!
//! [`GCRefCountedPtr<T>`]: https://github.com/TuGraph-family/tugraph-db/blob/dc8f9b479f9ded9020536dd395903d9855191a58/src/core/managed_object.h#L152
//! [`ScopedRef<T>`]: https://github.com/TuGraph-family/tugraph-db/blob/dc8f9b479f9ded9020536dd395903d9855191a58/src/core/managed_object.h#LL94C7-L94C16
//! [`TimedTaskScheduler`]: https://github.com/TuGraph-family/fma-common/blob/7007036315e861e1d53174784592c337c22cbeb9/fma-common/timed_task.h#L88
//! [`destructor`]: https://github.com/TuGraph-family/fma-common/blob/7007036315e861e1d53174784592c337c22cbeb9/fma-common/timed_task.h#L118

pub mod cursor;
pub mod db;
pub mod field;
pub mod index;
mod raw;
// pub mod rc;
pub mod role_info;
pub mod txn;
pub mod types;
pub mod user_info;
use libtugraph_sys as ffi;
use std::{error, fmt, result};

/// `ErrorKind` ports all exceptions from lgraph_exceptions.h
///
/// > **Note:** Some expections not in lgraph_exceptions.h, for example std::runtime_error("custom error msg"),
/// > are ported as Unknown
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ErrorKind {
    // InvalidParameterError
    InvalidParameter,
    // OutOfRangeError
    OutOfRange,
    // InvalidGalaxyError
    InvalidGalaxy,
    // InvalidGraphDBError
    InvalidGraphDB,
    // InvalidTxnError
    InvalidTxn,
    // InvalidIteratorError
    InvalidIterator,
    // InvalidForkError
    InvalidFork,
    // TxnConflictError
    TxnConflict,
    // WriteNotAllowedError,
    WriteNotAllowed,
    // DBNotExistError
    DBNotExist,
    // IOError
    IOError,
    // UnauthorizedError
    Unauthorized,
    // Errors not in lgraph_exceptions.h but from C++ std::exception
    // e.g. OutOfBound("whose msg is variant"), std::runtime_error("custom error msg")
    Other,
}

/// `Error` contains the message from what() yield by C++  std::exception
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    message: String,
}

impl Error {
    fn new(message: String) -> Error {
        Error { message }
    }

    /// Converts `Error` into a `String`.
    pub fn into_string(self) -> String {
        self.msg().to_string()
    }

    /// Get the message contained in `Error`.
    pub fn msg(&self) -> &str {
        &self.message
    }

    /// Parse corresponding [`ErrorKind`] from message contained in `Error`.
    pub fn kind(&self) -> ErrorKind {
        match self.message.as_str() {
            "Invalid parameter." => ErrorKind::InvalidParameter,
            "Invalid Galaxy." => ErrorKind::InvalidGalaxy,
            "Invalid GraphDB." => ErrorKind::InvalidGraphDB,
            "Invalid transaction." => ErrorKind::InvalidTxn,
            "Invalid iterator." => ErrorKind::InvalidIterator,
            "Write transactions cannot be forked." => ErrorKind::InvalidFork,
            "Transaction conflicts with an earlier one." => ErrorKind::TxnConflict,
            "Access denied." => ErrorKind::WriteNotAllowed,
            "The specified TuGraph DB does not exist." => ErrorKind::DBNotExist,
            "IO Error." => ErrorKind::IOError,
            "Unauthorized." => ErrorKind::Unauthorized,
            _ => ErrorKind::Other,
        }
    }
}

impl AsRef<str> for Error {
    fn as_ref(&self) -> &str {
        &self.message
    }
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}

pub type Result<T> = result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error() {
        [
            ("Invalid parameter.", ErrorKind::InvalidParameter),
            ("Invalid Galaxy.", ErrorKind::InvalidGalaxy),
            ("Invalid GraphDB.", ErrorKind::InvalidGraphDB),
            ("Invalid transaction.", ErrorKind::InvalidTxn),
            ("Invalid iterator.", ErrorKind::InvalidIterator),
            (
                "Write transactions cannot be forked.",
                ErrorKind::InvalidFork,
            ),
            (
                "Transaction conflicts with an earlier one.",
                ErrorKind::TxnConflict,
            ),
            ("Access denied.", ErrorKind::WriteNotAllowed),
            (
                "The specified TuGraph DB does not exist.",
                ErrorKind::DBNotExist,
            ),
            ("IO Error.", ErrorKind::IOError),
            ("Unauthorized.", ErrorKind::Unauthorized),
            ("Other Unkown error.", ErrorKind::Other),
        ]
        .into_iter()
        .for_each(|(msg, kind)| {
            let e = Error::new(msg.into());
            assert_eq!(e.as_ref(), msg);
            assert_eq!(e.msg(), msg);
            assert_eq!(e.kind(), kind);
        })
    }
}
