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

use libc::c_char;
use std::{collections::HashMap, ptr, slice};

use crate::{ffi, types::AccessLevel};

use super::ffi_util;

raw_pod_rustlize!(
    RawRoleInfo,
    lgraph_api_role_info_t,
    lgraph_api_role_info_destroy,
);

impl RawRoleInfo {
    pub(crate) fn desc(&self) -> String {
        unsafe {
            let cstr = ffi::lgraph_api_role_info_get_desc(self.inner);
            ffi_util::to_rust_string(cstr)
        }
    }

    pub(crate) fn graph_access(&self) -> HashMap<String, AccessLevel> {
        unsafe {
            let mut cgraph_names: *mut *mut c_char = ptr::null_mut();
            let mut caccess_levels: *mut i32 = ptr::null_mut();
            let len = ffi::lgraph_api_role_info_get_graph_access(
                self.inner,
                &mut cgraph_names as *mut _,
                &mut caccess_levels as *mut _,
            );
            let graph_names: Vec<_> = slice::from_raw_parts(cgraph_names, len)
                .iter()
                .map(|s| ffi_util::to_rust_string(*s))
                .collect();
            let access_levels: Vec<_> = slice::from_raw_parts(caccess_levels, len)
                .iter()
                .map(|l| AccessLevel::try_from(*l as u32).unwrap())
                .collect();
            graph_names
                .into_iter()
                .zip(access_levels.into_iter())
                .collect()
        }
    }

    pub(crate) fn disabled(&self) -> bool {
        unsafe { ffi::lgraph_api_role_info_get_disabled(self.inner) }
    }
}
