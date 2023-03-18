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

use crate::{ffi, field::FieldType};

use super::ffi_util::{self, CStrLike};

raw_pod_rustlize!(
    RawFieldSpec,
    lgraph_api_field_spec_t,
    lgraph_api_field_spec_destroy,
);

impl RawFieldSpec {
    pub(crate) fn from_name_ty_optional<T: CStrLike>(name: T, ty: u32, optional: bool) -> Self {
        let cname = name.into_c_string().unwrap();
        unsafe {
            RawFieldSpec {
                inner: ffi::lgraph_api_create_field_spec_name_type_optional(
                    cname.as_ptr(),
                    ty as i32,
                    optional,
                ),
            }
        }
    }
    pub(crate) fn name(&self) -> String {
        unsafe {
            let cstr = ffi::lgraph_api_field_spec_get_name(self.inner);
            ffi_util::from_cstr(cstr)
        }
    }

    pub(crate) fn ty(&self) -> FieldType {
        unsafe {
            FieldType::try_from(ffi::lgraph_api_field_spec_get_type(self.inner) as u32).unwrap()
        }
    }

    pub(crate) fn optional(&self) -> bool {
        unsafe { ffi::lgraph_api_field_spec_get_optional(self.inner) }
    }
}
