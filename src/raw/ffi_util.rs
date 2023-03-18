// Copyright 2016 Alex Regueiro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Thanks great ffi_utils of rust-rocksdb
// (https://github.com/rust-rocksdb/rust-rocksdb)
//

#![allow(dead_code)]

use crate::Error;
use libc::{self, c_char, c_void};
use std::ffi::{CStr, CString};
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr;

pub unsafe fn from_cstr(ptr: *const c_char) -> String {
    let cstr = CStr::from_ptr(ptr as *const _);
    String::from_utf8_lossy(cstr.to_bytes()).into_owned()
}

pub unsafe fn to_rust_string(ptr: *const c_char) -> String {
    let s = from_cstr(ptr);
    libc::free(ptr as *mut c_void);
    s
}

pub unsafe fn raw_data(ptr: *const c_char, size: usize) -> Option<Vec<u8>> {
    if ptr.is_null() {
        None
    } else {
        let mut dst = vec![0; size];
        ptr::copy_nonoverlapping(ptr as *const u8, dst.as_mut_ptr(), size);

        Some(dst)
    }
}

pub fn error_message(ptr: *const c_char) -> String {
    unsafe { to_rust_string(ptr) }
}

pub fn opt_bytes_to_ptr<T: AsRef<[u8]>>(opt: Option<T>) -> *const c_char {
    match opt {
        Some(v) => v.as_ref().as_ptr() as *const c_char,
        None => ptr::null(),
    }
}

pub fn to_cpath<P: AsRef<Path>>(path: P) -> Result<CString, Error> {
    match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
        Ok(c) => Ok(c),
        Err(e) => Err(Error::new(format!(
            "Failed to convert path to CString: {e}"
        ))),
    }
}

macro_rules! ffi_try {
    ( $($function:ident)::*() ) => {
        ffi_try_impl!($($function)::*())
    };

    ( $($function:ident)::*( $arg1:expr $(, $arg:expr)* $(,)? ) ) => {
        ffi_try_impl!($($function)::*($arg1 $(, $arg)* ,))
    };
}

macro_rules! ffi_try_impl {
    ( $($function:ident)::*( $($arg:expr,)*) ) => {{
        let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
        let result = $($function)::*($($arg,)* &mut err);
        if !err.is_null() {
            Err($crate::Error::new($crate::raw::ffi_util::error_message(err)))
        } else {
            Ok(result)
        }
    }};
}

pub(crate) trait CStrLike {
    type Baked: std::ops::Deref<Target = CStr>;
    type Error: std::fmt::Debug + std::fmt::Display;

    /// Bakes self into value which can be freely converted into [`&CStr`](CStr).
    ///
    /// This may require allocation and may fail if `self` has invalid value.
    fn bake(self) -> Result<Self::Baked, Self::Error>;

    /// Consumers and converts value into an owned [`CString`].
    ///
    /// If `Self` is already a `CString` simply returns it; if it’s a reference
    /// to a `CString` then the value is cloned.  In other cases this may
    /// require allocation and may fail if `self` has invalid value.
    fn into_c_string(self) -> Result<CString, Self::Error>;
}

impl CStrLike for &str {
    type Baked = CString;
    type Error = std::ffi::NulError;

    fn bake(self) -> Result<Self::Baked, Self::Error> {
        CString::new(self)
    }
    fn into_c_string(self) -> Result<CString, Self::Error> {
        CString::new(self)
    }
}

// This is redundant for the most part and exists so that `foo(&string)` (where
// `string: String` works just as if `foo` took `arg: &str` argument.
impl CStrLike for &String {
    type Baked = CString;
    type Error = std::ffi::NulError;

    fn bake(self) -> Result<Self::Baked, Self::Error> {
        CString::new(self.as_bytes())
    }
    fn into_c_string(self) -> Result<CString, Self::Error> {
        CString::new(self.as_bytes())
    }
}

impl CStrLike for &CStr {
    type Baked = Self;
    type Error = std::convert::Infallible;

    fn bake(self) -> Result<Self::Baked, Self::Error> {
        Ok(self)
    }
    fn into_c_string(self) -> Result<CString, Self::Error> {
        Ok(self.to_owned())
    }
}

// This exists so that if caller constructs a `CString` they can pass it into
// the function accepting `CStrLike` argument.  Some of such functions may take
// the argument whereas otherwise they would need to allocated a new owned
// object.
impl CStrLike for CString {
    type Baked = CString;
    type Error = std::convert::Infallible;

    fn bake(self) -> Result<Self::Baked, Self::Error> {
        Ok(self)
    }
    fn into_c_string(self) -> Result<CString, Self::Error> {
        Ok(self)
    }
}

// This is redundant for the most part and exists so that `foo(&cstring)` (where
// `string: CString` works just as if `foo` took `arg: &CStr` argument.
impl<'a> CStrLike for &'a CString {
    type Baked = &'a CStr;
    type Error = std::convert::Infallible;

    fn bake(self) -> Result<Self::Baked, Self::Error> {
        Ok(self)
    }
    fn into_c_string(self) -> Result<CString, Self::Error> {
        Ok(self.clone())
    }
}

impl CStrLike for &Path {
    type Baked = CString;
    type Error = std::ffi::NulError;

    fn bake(self) -> Result<Self::Baked, Self::Error> {
        CString::new(self.as_os_str().as_bytes())
    }

    fn into_c_string(self) -> Result<CString, Self::Error> {
        CString::new(self.as_os_str().as_bytes())
    }
}

impl CStrLike for &PathBuf {
    type Baked = CString;
    type Error = std::ffi::NulError;

    fn bake(self) -> Result<Self::Baked, Self::Error> {
        CString::new(self.as_os_str().as_bytes())
    }

    fn into_c_string(self) -> Result<CString, Self::Error> {
        CString::new(self.as_os_str().as_bytes())
    }
}

#[test]
fn test_c_str_like_bake() {
    fn test<S: CStrLike>(value: S) -> Result<usize, S::Error> {
        value
            .bake()
            .map(|value| unsafe { libc::strlen(value.as_ptr()) })
    }

    assert_eq!(Ok(3), test("foo")); // &str
    assert_eq!(Ok(3), test(&String::from("foo"))); // String
    assert_eq!(Ok(3), test(CString::new("foo").unwrap().as_ref())); // &CStr
    assert_eq!(Ok(3), test(&CString::new("foo").unwrap())); // &CString
    assert_eq!(Ok(3), test(CString::new("foo").unwrap())); // CString

    assert_eq!(3, test("foo\0bar").err().unwrap().nul_position());
}

#[test]
fn test_c_str_like_into() {
    fn test<S: CStrLike>(value: S) -> Result<CString, S::Error> {
        value.into_c_string()
    }

    let want = CString::new("foo").unwrap();

    assert_eq!(Ok(want.clone()), test("foo")); // &str
    assert_eq!(Ok(want.clone()), test(&String::from("foo"))); // &String
    assert_eq!(
        Ok(want.clone()),
        test(CString::new("foo").unwrap().as_ref())
    ); // &CStr
    assert_eq!(Ok(want.clone()), test(&CString::new("foo").unwrap())); // &CString
    assert_eq!(Ok(want), test(CString::new("foo").unwrap())); // CString

    assert_eq!(3, test("foo\0bar").err().unwrap().nul_position());
}
