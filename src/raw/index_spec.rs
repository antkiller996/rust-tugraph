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

use crate::ffi;

use super::ffi_util;

raw_pod_rustlize!(
    RawIndexSpec,
    lgraph_api_index_spec_t,
    lgraph_api_index_spec_destroy,
);

impl RawIndexSpec {
    pub(crate) fn label(&self) -> String {
        unsafe {
            let cstr = ffi::lgraph_api_index_spec_get_label(self.inner);
            ffi_util::from_cstr(cstr)
        }
    }

    pub(crate) fn field(&self) -> String {
        unsafe {
            let cstr = ffi::lgraph_api_index_spec_get_field(self.inner);
            ffi_util::from_cstr(cstr)
        }
    }

    pub(crate) fn unique(&self) -> bool {
        unsafe { ffi::lgraph_api_index_spec_get_unique(self.inner) }
    }
}
