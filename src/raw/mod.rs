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

#![allow(unused)]

macro_rules! raw_core_cursor_rustlize {
    (
        $cursor_name:ident,
        $cursor_ctype:ident,
        $cursor_close:ident,
        $cursor_destroy:ident,
        $cursor_is_valid:ident,
        $cursor_next:ident$(,)?
    ) => {
        /// RAIIize the unsafe c bindings and Rustlize all types cross api boundary.
        pub(crate) struct $cursor_name {
            inner: *mut $crate::ffi::$cursor_ctype,
        }

        impl Drop for $cursor_name {
            fn drop(&mut self) {
                unsafe {
                    $crate::ffi::$cursor_close(self.inner);
                    $crate::ffi::$cursor_destroy(self.inner);
                }
            }
        }

        impl $cursor_name {
            ///
            ///
            /// # Safety
            ///
            /// The `ptr` passed in must be a valid and non-null pointer
            /// created by libtugraph_sys::lgraph_api_transaction_get_vertex_iterator or
            /// libtugraph_sys::lgraph_api_vertex_iterator_get_out_edge_iterator and their
            /// variants
            pub(crate) unsafe fn from_ptr(ptr: *mut $crate::ffi::$cursor_ctype) -> Self {
                $cursor_name { inner: ptr }
            }
            pub(crate) fn is_valid(&self) -> bool {
                unsafe { $crate::ffi::$cursor_is_valid(self.inner) }
            }

            pub(crate) fn next(&self) -> $crate::Result<bool> {
                unsafe {
                    ffi_try! {$crate::ffi::$cursor_next(self.inner)}
                }
            }
        }
    };
}

macro_rules! raw_core_cursor_primary_ext_rustlize {
    (
        $cursor_name:ident,
        $cursor_get_label_id:ident
        $(
        ,
        $cursor_get_label:ident,
        $cursor_get_fields_by_names:ident,
        $cursor_get_field_by_name:ident,
        $cursor_get_fields_by_ids:ident,
        $cursor_get_field_by_id:ident,
        $cursor_get_all_fields:ident,
        $cursor_set_field_by_name:ident,
        $cursor_set_field_by_id:ident,
        $cursor_set_fields_by_value_strings:ident,
        $cursor_set_fields_by_data:ident,
        $cursor_set_fields_by_ids:ident
        )?
        $(,)?
    ) => {
        impl $cursor_name {
            pub(crate) fn get_label_id(&self) -> $crate::Result<u16> {
                unsafe {
                    ffi_try! { $crate::ffi::$cursor_get_label_id(self.inner) }
                }
            }

            $(
            pub(crate) fn get_label(&self) -> $crate::Result<String> {
                unsafe {
                    ffi_try! { $crate::ffi::$cursor_get_label(self.inner) }
                        .map(|l| $crate::raw::ffi_util::from_cstr(l))
                }
            }

            pub(crate) fn get_fields_by_names<T: $crate::raw::ffi_util::CStrLike + Copy>(
                &self,
                names: &[T],
            ) -> $crate::Result<Vec<super::RawFieldData>> {
                unsafe {
                    let cnames: Vec<_> = names
                        .iter()
                        .map(|cstr| cstr.into_c_string().unwrap())
                        .collect();
                    let cnames: Vec<*const libc::c_char> =
                        cnames.iter().map(|v| v.as_ptr()).collect();
                    let mut cfield_datas: *mut *mut $crate::ffi::lgraph_api_field_data_t =
                        std::ptr::null_mut();
                    let len = ffi_try! {
                        $crate::ffi::$cursor_get_fields_by_names(
                            self.inner,
                            cnames.as_ptr(),
                            cnames.len(),
                            &mut cfield_datas as *mut _,
                        )
                    }?;
                    let field_datas: Vec<_> = std::slice::from_raw_parts(cfield_datas, len)
                        .iter()
                        .map(|ptr| super::RawFieldData::from_ptr(*ptr))
                        .collect();
                    Ok(field_datas)
                }
            }

            pub(crate) fn get_field_by_name<T: $crate::raw::ffi_util::CStrLike>(
                &self,
                name: T,
            ) -> $crate::Result<super::RawFieldData> {
                unsafe {
                    let cname = name.into_c_string().unwrap();
                    ffi_try! {
                        $crate::ffi::$cursor_get_field_by_name(self.inner, cname.as_ptr())
                    }
                    .map(|ptr| unsafe { super::RawFieldData::from_ptr(ptr) })
                }
            }

            pub(crate) fn get_fields_by_ids(
                &self,
                ids: &[usize],
            ) -> $crate::Result<Vec<super::RawFieldData>> {
                unsafe {
                    let mut cfield_datas: *mut *mut $crate::ffi::lgraph_api_field_data_t =
                        std::ptr::null_mut();
                    let len = ffi_try! {
                        $crate::ffi::$cursor_get_fields_by_ids(
                            self.inner,
                            ids.as_ptr(),
                            ids.len(),
                            &mut cfield_datas as *mut _,
                        )
                    }?;
                    let field_datas: Vec<_> = std::slice::from_raw_parts(cfield_datas, len)
                        .iter()
                        .map(|ptr| super::RawFieldData::from_ptr(*ptr))
                        .collect();
                    Ok(field_datas)
                }
            }

            pub(crate) fn get_field_by_id(
                &self,
                id: usize,
            ) -> $crate::Result<super::RawFieldData> {
                unsafe {
                    ffi_try! {
                        $crate::ffi::$cursor_get_field_by_id(self.inner, id)
                    }
                    .map(|ptr| unsafe { super::RawFieldData::from_ptr(ptr) })
                }
            }

            pub(crate) fn get_all_fields(
                &self,
            ) -> $crate::Result<(Vec<String>, Vec<super::RawFieldData>)> {
                unsafe {
                    let mut cfield_names: *mut *mut libc::c_char = std::ptr::null_mut();
                    let mut cfield_datas: *mut *mut $crate::ffi::lgraph_api_field_data_t =
                        std::ptr::null_mut();

                    let len = ffi_try! {
                        $crate::ffi::$cursor_get_all_fields(
                            self.inner,
                            &mut cfield_names as *mut _,
                            &mut cfield_datas as *mut _,
                        )
                    }?;
                    let names: Vec<_> = std::slice::from_raw_parts(cfield_names, len)
                        .iter()
                        .map(|ptr| $crate::raw::ffi_util::from_cstr(*ptr))
                        .collect();
                    let field_datas: Vec<_> = std::slice::from_raw_parts(cfield_datas, len)
                        .iter()
                        .map(|ptr| super::RawFieldData::from_ptr(*ptr))
                        .collect();
                    $crate::ffi::lgraph_api_field_names_destroy(cfield_names, len);
                    Ok((
                        names,
                        field_datas,
                    ))
                }
            }

            pub(crate) fn set_field_by_name<T: $crate::raw::ffi_util::CStrLike>(
                &self,
                name: T,
                data: &super::RawFieldData,
            ) -> $crate::Result<()> {
                unsafe {
                    let cname = name.into_c_string().unwrap();
                    ffi_try! {
                        $crate::ffi::$cursor_set_field_by_name(
                            self.inner,
                            cname.as_ptr(),
                            data.as_ptr(),
                        )
                    }
                }
            }

            pub(crate) fn set_field_by_id(
                &self,
                id: usize,
                data: &super::RawFieldData,
            ) -> $crate::Result<()> {
                unsafe {
                    ffi_try! {
                        $crate::ffi::$cursor_set_field_by_id(
                            self.inner,
                            id,
                            data.as_ptr(),
                        )
                    }
                }
            }

            pub(crate) fn set_fields_by_value_strings<T: $crate::raw::ffi_util::CStrLike + Copy>(
                &self,
                names: &[T],
                value_strings: &[T],
            ) -> $crate::Result<()> {
                unsafe {
                    let cnames: Vec<_> = names
                        .iter()
                        .map(|cstr| cstr.into_c_string().unwrap())
                        .collect();
                    let cnames: Vec<*const libc::c_char> =
                        cnames.iter().map(|v| v.as_ptr()).collect();
                    let cvalue_strings: Vec<_> = value_strings
                        .iter()
                        .map(|cstr| cstr.into_c_string().unwrap())
                        .collect();
                    let cvalue_strings: Vec<*const libc::c_char> =
                        cvalue_strings.iter().map(|s| s.as_ptr()).collect();
                    ffi_try! {
                        $crate::ffi::$cursor_set_fields_by_value_strings(
                            self.inner,
                            cnames.as_ptr(),
                            cnames.len(),
                            cvalue_strings.as_ptr(),
                            cvalue_strings.len(),
                        )
                    }
                }
            }

            pub(crate) fn set_fields_by_data<'a, T, N, V>(
                &self,
                names: N,
                values: V,
            ) -> $crate::Result<()>
            where
                T: $crate::raw::ffi_util::CStrLike + Copy,
                N: std::iter::IntoIterator<Item = T>,
                V: std::iter::IntoIterator<Item = &'a super::RawFieldData>,
            {
                unsafe {
                    let cnames: Vec<_> = names
                        .into_iter()
                        .map(|cstr| cstr.into_c_string().unwrap())
                        .collect();
                    let cnames: Vec<*const libc::c_char> =
                        cnames.iter().map(|n| n.as_ptr()).collect();
                    let cfield_values: Vec<_> = values.into_iter().map(|s| s.as_ptr()).collect();
                    ffi_try! {
                        $crate::ffi::$cursor_set_fields_by_data(
                            self.inner,
                            cnames.as_ptr(),
                            cnames.len(),
                            cfield_values.as_ptr(),
                            cfield_values.len(),
                        )
                    }
                }
            }

            pub(crate) fn set_fields_by_ids<'a, T>(
                &self,
                ids: &[usize],
                values: T,
            ) -> $crate::Result<()>
            where
                T: std::iter::IntoIterator<Item = &'a super::RawFieldData>
            {
                unsafe {
                    let cfield_values: Vec<_> = values.into_iter().map(|s| s.as_ptr()).collect();
                    ffi_try! {
                        $crate::ffi::$cursor_set_fields_by_ids(
                            self.inner,
                            ids.as_ptr(),
                            ids.len(),
                            cfield_values.as_ptr(),
                            cfield_values.len(),
                        )
                    }
                }
            }
            )?
        }
    };
}

macro_rules! raw_primary_cursor_rustlize {
    (
        $cursor_name:ident,
        $cursor_ctype:ident,
        $cursor_close:ident,
        $cursor_destroy:ident,
        $cursor_is_valid:ident,
        $cursor_next:ident,
        $cursor_get_label:ident,
        $cursor_get_label_id:ident,
        $cursor_get_fields_by_names:ident,
        $cursor_get_field_by_name:ident,
        $cursor_get_fields_by_ids:ident,
        $cursor_get_field_by_id:ident,
        $cursor_get_all_fields:ident,
        $cursor_set_field_by_name:ident,
        $cursor_set_field_by_id:ident,
        $cursor_set_fields_by_value_strings:ident,
        $cursor_set_fields_by_data:ident,
        $cursor_set_fields_by_ids:ident$(,)?
    ) => {
        raw_core_cursor_rustlize!(
            $cursor_name,
            $cursor_ctype,
            $cursor_close,
            $cursor_destroy,
            $cursor_is_valid,
            $cursor_next,
        );

        raw_core_cursor_primary_ext_rustlize!(
            $cursor_name,
            $cursor_get_label_id,
            $cursor_get_label,
            $cursor_get_fields_by_names,
            $cursor_get_field_by_name,
            $cursor_get_fields_by_ids,
            $cursor_get_field_by_id,
            $cursor_get_all_fields,
            $cursor_set_field_by_name,
            $cursor_set_field_by_id,
            $cursor_set_fields_by_value_strings,
            $cursor_set_fields_by_data,
            $cursor_set_fields_by_ids,
        );
    };
}

macro_rules! raw_pod_rustlize {
    (
        $pod_name:ident,
        $pod_ctype:ident,
        $pod_destroy:ident$(,)?
    ) => {
        pub(crate) struct $pod_name {
            inner: *mut $crate::ffi::$pod_ctype,
        }

        impl $pod_name {
            ///
            /// # Safety
            ///
            /// The `ptr` passed in must be valid and non-null
            /// created by libtugraph_sys::lgraph_api_{TYPE}_create or libtugraph_sys::lgraph_api_{CLASS}_get_{TYPE}
            /// where {CLASS} is a name of class and {TYPE} is a name of this type
            pub(crate) unsafe fn from_ptr(ptr: *mut $crate::ffi::$pod_ctype) -> Self {
                $pod_name { inner: ptr }
            }

            pub(crate) fn as_ptr(&self) -> *const $crate::ffi::$pod_ctype {
                self.inner
            }
            pub(crate) fn into_ptr(self) -> *mut $crate::ffi::$pod_ctype {
                self.inner
            }

            pub(crate) unsafe fn as_ptr_mut(&self) -> *mut $crate::ffi::$pod_ctype {
                self.inner
            }
        }

        impl Drop for $pod_name {
            fn drop(&mut self) {
                unsafe {
                    $crate::ffi::$pod_destroy(self.inner);
                }
            }
        }
    };
}

#[macro_use]
mod ffi_util;
mod db;
mod edge;
mod field_data;
mod field_spec;
mod galaxy;
mod index_spec;
mod role_info;
mod txn;
mod types;
mod user_info;
mod vertex;

pub(crate) use db::RawGraphDB;
pub(crate) use edge::{RawEdgeCursor, RawEdgeIndexIterator, RawInEdgeCursor, RawOutEdgeCursor};
pub(crate) use field_data::RawFieldData;
pub(crate) use field_spec::RawFieldSpec;
pub(crate) use galaxy::RawGalaxy;
pub(crate) use index_spec::RawIndexSpec;
pub(crate) use role_info::RawRoleInfo;
pub(crate) use txn::RawTransaction;
pub(crate) use types::{RawDate, RawDateTime, RawEdgeUid};
pub(crate) use user_info::RawUserInfo;
pub(crate) use vertex::{RawVertexCursor, RawVertexIndexIterator};
