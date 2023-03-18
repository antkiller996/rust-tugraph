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

use super::ffi_util;
use crate::ffi;
use libc::c_char;
use std::{collections::HashSet, ptr, slice};

raw_pod_rustlize!(
    RawUserInfo,
    lgraph_api_user_info_t,
    lgraph_api_user_info_destroy,
);

impl RawUserInfo {
    pub(crate) fn desc(&self) -> String {
        unsafe {
            let cstr = ffi::lgraph_api_user_info_get_desc(self.inner);
            ffi_util::to_rust_string(cstr)
        }
    }

    pub(crate) fn roles(&self) -> HashSet<String> {
        unsafe {
            let mut roles: *mut *mut c_char = ptr::null_mut();
            let len = ffi::lgraph_api_user_info_get_roles(self.inner, &mut roles as *mut _);
            slice::from_raw_parts(roles, len)
                .iter()
                .map(|cstr| ffi_util::to_rust_string(*cstr))
                .collect()
        }
    }

    pub(crate) fn disabled(&self) -> bool {
        unsafe { ffi::lgraph_api_user_info_get_disable(self.inner) }
    }

    pub(crate) fn memory_limit(&self) -> usize {
        unsafe { ffi::lgraph_api_user_info_get_memory_limit(self.inner) }
    }
}
