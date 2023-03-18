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

use crate::field::FieldType;

use crate::ffi;

use super::{
    ffi_util::{self, CStrLike},
    types::{RawDate, RawDateTime},
};

raw_pod_rustlize!(
    RawFieldData,
    lgraph_api_field_data_t,
    lgraph_api_field_data_destroy,
);

impl RawFieldData {
    pub(crate) fn new() -> Self {
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data(),
            }
        }
    }

    pub(crate) fn from_bool(b: bool) -> Self {
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_bool(b),
            }
        }
    }

    pub(crate) fn from_int8(i: i8) -> Self {
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_int8(i),
            }
        }
    }

    pub(crate) fn from_int16(i: i16) -> Self {
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_int16(i),
            }
        }
    }

    pub(crate) fn from_int32(i: i32) -> Self {
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_int32(i),
            }
        }
    }

    pub(crate) fn from_int64(i: i64) -> Self {
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_int64(i),
            }
        }
    }

    pub(crate) fn from_float(f: f32) -> Self {
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_float(f),
            }
        }
    }

    pub(crate) fn from_double(d: f64) -> Self {
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_double(d),
            }
        }
    }

    pub(crate) fn from_date(date: RawDate) -> Self {
        unsafe {
            // SAFETY: date should be dropped after creating a new date
            // new date is created by copy-ctor of underlying repr of lgraph_data_t
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_date(date.as_ptr_mut()),
            }
        }
    }

    pub(crate) fn from_datetime(datetime: RawDateTime) -> Self {
        unsafe {
            // SAFETY: datetime should be dropped after creating a new datetime
            // new datetime is created by copy-ctor of underlying repr of lgraph_data_time_t
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_date_time(datetime.as_ptr_mut()),
            }
        }
    }

    pub(crate) fn from_str<T: CStrLike>(cstr: T) -> Self {
        let cstring = cstr.into_c_string().unwrap();
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_str(cstring.as_ptr()),
            }
        }
    }

    pub(crate) fn from_blob<T: AsRef<[u8]>>(blob: T) -> Self {
        let blob = blob.as_ref();
        unsafe {
            RawFieldData {
                inner: ffi::lgraph_api_create_field_data_blob(blob.as_ptr(), blob.len()),
            }
        }
    }

    pub(crate) fn ty(&self) -> FieldType {
        unsafe {
            let ty = ffi::lgraph_api_field_data_get_type(self.inner);
            FieldType::try_from(ty as u32).unwrap()
        }
    }

    pub(crate) unsafe fn as_bool_unchecked(&self) -> bool {
        ffi::lgraph_api_field_data_as_bool(self.inner)
    }

    pub(crate) unsafe fn as_int8_unchecked(&self) -> i8 {
        ffi::lgraph_api_field_data_as_int8(self.inner)
    }

    pub(crate) unsafe fn as_int16_unchecked(&self) -> i16 {
        ffi::lgraph_api_field_data_as_int16(self.inner)
    }

    pub(crate) unsafe fn as_int32_unchecked(&self) -> i32 {
        ffi::lgraph_api_field_data_as_int32(self.inner)
    }

    pub(crate) unsafe fn as_int64_unchecked(&self) -> i64 {
        ffi::lgraph_api_field_data_as_int64(self.inner)
    }

    pub(crate) unsafe fn as_float_unchecked(&self) -> f32 {
        ffi::lgraph_api_field_data_as_float(self.inner)
    }

    pub(crate) unsafe fn as_double_unchecked(&self) -> f64 {
        ffi::lgraph_api_field_data_as_double(self.inner)
    }

    pub(crate) unsafe fn as_date_unchecked(&self) -> RawDate {
        let ptr = ffi::lgraph_api_field_data_as_date(self.inner);
        RawDate::from_ptr(ptr)
    }

    pub(crate) unsafe fn as_datetime_unchecked(&self) -> RawDateTime {
        let ptr = ffi::lgraph_api_field_data_as_date_time(self.inner);
        RawDateTime::from_ptr(ptr)
    }

    pub(crate) unsafe fn as_string_unchecked(&self) -> String {
        let cstr = ffi::lgraph_api_field_data_as_str(self.inner);
        ffi_util::to_rust_string(cstr)
    }

    pub(crate) unsafe fn as_blob_unchecked(&self) -> Vec<u8> {
        let cstr = ffi::lgraph_api_field_data_as_blob(self.inner);
        ffi_util::to_rust_string(cstr).into_bytes()
    }
}

impl Default for RawFieldData {
    fn default() -> Self {
        Self::new()
    }
}
