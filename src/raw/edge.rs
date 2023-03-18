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

use super::{RawEdgeUid, RawFieldData};
use crate::Result;

macro_rules! raw_cursor_edge_ext_rustlize {
    (
        $cursor_name:ident,
        $cursor_get_uid:ident,
        $cursor_get_dst:ident,
        $cursor_get_edge_id:ident,
        $cursor_get_src:ident
        $(
            ,
            $cursor_goto:ident,
            $cursor_get_temporal_id:ident,
            $cursor_delete:ident
        )?
        $(,)?
    ) => {
        impl $cursor_name {
            pub(crate) fn get_uid(&self) -> $crate::Result<super::RawEdgeUid> {
                unsafe {
                    ffi_try! { $crate::ffi::$cursor_get_uid(self.inner)}
                        .map(|ptr| unsafe { super::RawEdgeUid::from_ptr(ptr) })
                }
            }

            pub(crate) fn get_dst(&self) -> $crate::Result<i64> {
                unsafe {
                    ffi_try! { $crate::ffi::$cursor_get_dst(self.inner) }
                }
            }

            pub(crate) fn get_edge_id(&self) -> $crate::Result<i64> {
                unsafe {
                    ffi_try! { $crate::ffi::$cursor_get_edge_id(self.inner) }
                }
            }

            pub(crate) fn get_src(&self) -> $crate::Result<i64> {
                unsafe {
                    ffi_try! { $crate::ffi::$cursor_get_src(self.inner) }
                }
            }

            $(
            pub(crate) fn goto(
                &self,
                euid: &super::RawEdgeUid,
                nearest: bool,
            ) -> $crate::Result<bool> {
                unsafe {
                    ffi_try! {$crate::ffi::$cursor_goto(
                        self.inner, euid.as_ptr(), nearest
                    )}
                }
            }

            pub(crate) fn get_temporal_id(&self) -> $crate::Result<i64> {
                unsafe {
                    ffi_try! { $crate::ffi::$cursor_get_temporal_id(self.inner) }
                }
            }

            pub(crate) fn delete(&self) -> $crate::Result<()> {
                unsafe {
                    ffi_try! {
                        $crate::ffi::$cursor_delete(self.inner)
                    }
                }
            }
            )?
        }
    };
}

macro_rules! raw_edge_cursor_rustlize {
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
        $cursor_set_fields_by_ids:ident,
        $cursor_goto:ident,
        $cursor_get_uid:ident,
        $cursor_get_dst:ident,
        $cursor_get_edge_id:ident,
        $cursor_get_temporal_id:ident,
        $cursor_get_src:ident,
        $cursor_delete:ident$(,)?
    ) => {
        raw_primary_cursor_rustlize!(
            $cursor_name,
            $cursor_ctype,
            $cursor_close,
            $cursor_destroy,
            $cursor_is_valid,
            $cursor_next,
            $cursor_get_label,
            $cursor_get_label_id,
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
        raw_cursor_edge_ext_rustlize!(
            $cursor_name,
            $cursor_get_uid,
            $cursor_get_dst,
            $cursor_get_edge_id,
            $cursor_get_src,
            $cursor_goto,
            $cursor_get_temporal_id,
            $cursor_delete,
        );
    };
}

raw_edge_cursor_rustlize!(
    RawOutEdgeCursor,
    lgraph_api_out_edge_iterator_t,
    lgraph_api_out_edge_iterator_close,
    lgraph_api_out_edge_iterator_destroy,
    lgraph_api_out_edge_iterator_is_valid,
    lgraph_api_out_edge_iterator_next,
    lgraph_api_out_edge_iterator_get_label,
    lgraph_api_out_edge_iterator_get_label_id,
    lgraph_api_out_edge_iterator_get_fields_by_names,
    lgraph_api_out_edge_iterator_get_field_by_name,
    lgraph_api_out_edge_iterator_get_fields_by_ids,
    lgraph_api_out_edge_iterator_get_field_by_id,
    lgraph_api_out_edge_iterator_get_all_fields,
    lgraph_api_out_edge_iterator_set_field_by_name,
    lgraph_api_out_edge_iterator_set_field_by_id,
    lgraph_api_out_edge_iterator_set_fields_by_value_strings,
    lgraph_api_out_edge_iterator_set_fields_by_data,
    lgraph_api_out_edge_iterator_set_fields_by_ids,
    lgraph_api_out_edge_iterator_goto,
    lgraph_api_out_edge_iterator_get_uid,
    lgraph_api_out_edge_iterator_get_dst,
    lgraph_api_out_edge_iterator_get_edge_id,
    lgraph_api_out_edge_iterator_get_temporal_id,
    lgraph_api_out_edge_iterator_get_src,
    lgraph_api_out_edge_iterator_delete,
);

raw_edge_cursor_rustlize!(
    RawInEdgeCursor,
    lgraph_api_in_edge_iterator_t,
    lgraph_api_in_edge_iterator_close,
    lgraph_api_in_edge_iterator_destroy,
    lgraph_api_in_edge_iterator_is_valid,
    lgraph_api_in_edge_iterator_next,
    lgraph_api_in_edge_iterator_get_label,
    lgraph_api_in_edge_iterator_get_label_id,
    lgraph_api_in_edge_iterator_get_fields_by_names,
    lgraph_api_in_edge_iterator_get_field_by_name,
    lgraph_api_in_edge_iterator_get_fields_by_ids,
    lgraph_api_in_edge_iterator_get_field_by_id,
    lgraph_api_in_edge_iterator_get_all_fields,
    lgraph_api_in_edge_iterator_set_field_by_name,
    lgraph_api_in_edge_iterator_set_field_by_id,
    lgraph_api_in_edge_iterator_set_fields_by_value_strings,
    lgraph_api_in_edge_iterator_set_fields_by_data,
    lgraph_api_in_edge_iterator_set_fields_by_ids,
    lgraph_api_in_edge_iterator_goto,
    lgraph_api_in_edge_iterator_get_uid,
    lgraph_api_in_edge_iterator_get_dst,
    lgraph_api_in_edge_iterator_get_edge_id,
    lgraph_api_in_edge_iterator_get_temporal_id,
    lgraph_api_in_edge_iterator_get_src,
    lgraph_api_in_edge_iterator_delete,
);

macro_rules! raw_edge_index_cursor_rustlize {
    (
        $cursor_name:ident,
        $cursor_ctype:ident,
        $cursor_close:ident,
        $cursor_destroy:ident,
        $cursor_is_valid:ident,
        $cursor_next:ident,
        $cursor_get_index_value:ident,
        $cursor_get_uid:ident,
        $cursor_get_src:ident,
        $cursor_get_dst:ident,
        $cursor_get_label_id:ident,
        $cursor_get_edge_id:ident$(,)?
    ) => {
        raw_core_cursor_rustlize!(
            $cursor_name,
            $cursor_ctype,
            $cursor_close,
            $cursor_destroy,
            $cursor_is_valid,
            $cursor_next,
        );
        raw_core_cursor_primary_ext_rustlize!($cursor_name, $cursor_get_label_id);

        impl $cursor_name {
            pub(crate) fn get_index_value(&self) -> $crate::Result<super::RawFieldData> {
                unsafe {
                    ffi_try! {
                        $crate::ffi::lgraph_api_edge_index_iterator_get_index_value(self.inner)
                    }
                    .map(|ptr| unsafe { super::RawFieldData::from_ptr(ptr) })
                }
            }

            pub(crate) fn get_temporal_id(&self) -> $crate::Result<i64> {
                self.get_uid().map(|euid| euid.tid())
            }
        }

        raw_cursor_edge_ext_rustlize!(
            $cursor_name,
            $cursor_get_uid,
            $cursor_get_src,
            $cursor_get_dst,
            $cursor_get_edge_id,
        );
    };
}
raw_edge_index_cursor_rustlize!(
    RawEdgeIndexIterator,
    lgraph_api_edge_index_iterator_t,
    lgraph_api_edge_index_iterator_close,
    lgraph_api_edge_index_iterator_destroy,
    lgraph_api_edge_index_iterator_is_valid,
    lgraph_api_edge_index_iterator_next,
    lgraph_api_edge_index_iterator_get_index_value,
    lgraph_api_edge_index_iterator_get_uid,
    lgraph_api_edge_index_iterator_get_src,
    lgraph_api_edge_index_iterator_get_dst,
    lgraph_api_edge_index_iterator_get_label_id,
    lgraph_api_edge_index_iterator_get_edge_id,
);

macro_rules! raw_edge_cursor_impl {
    ($raw_cursor:ident) => {
        impl RawEdgeCursor for $raw_cursor {
            fn next(&self) -> Result<bool> {
                $raw_cursor::next(self)
            }

            fn goto(&self, euid: &RawEdgeUid, nearest: bool) -> Result<bool> {
                $raw_cursor::goto(self, euid, nearest)
            }

            fn get_uid(&self) -> Result<RawEdgeUid> {
                $raw_cursor::get_uid(self)
            }

            fn get_src(&self) -> Result<i64> {
                $raw_cursor::get_src(self)
            }

            fn get_dst(&self) -> Result<i64> {
                $raw_cursor::get_dst(self)
            }

            fn get_edge_id(&self) -> Result<i64> {
                $raw_cursor::get_edge_id(self)
            }

            fn get_temporal_id(&self) -> Result<i64> {
                $raw_cursor::get_temporal_id(self)
            }

            fn is_valid(&self) -> bool {
                $raw_cursor::is_valid(self)
            }

            fn get_label(&self) -> Result<String> {
                $raw_cursor::get_label(self)
            }

            fn get_label_id(&self) -> Result<u16> {
                $raw_cursor::get_label_id(self)
            }
            fn get_all_fields(&self) -> Result<(Vec<String>, Vec<RawFieldData>)> {
                $raw_cursor::get_all_fields(self)
            }

            fn get_fields_by_names(&self, names: &[&str]) -> Result<Vec<RawFieldData>> {
                $raw_cursor::get_fields_by_names(self, names)
            }

            fn get_field_by_name(&self, name: &str) -> Result<RawFieldData> {
                $raw_cursor::get_field_by_name(self, name)
            }

            fn get_fields_by_ids(&self, ids: &[usize]) -> Result<Vec<RawFieldData>> {
                $raw_cursor::get_fields_by_ids(self, ids)
            }

            fn get_field_by_id(&self, id: usize) -> Result<RawFieldData> {
                $raw_cursor::get_field_by_id(self, id)
            }

            fn set_field_by_name(&self, name: &str, value: &RawFieldData) -> Result<()> {
                $raw_cursor::set_field_by_name(self, name, value)
            }

            fn set_field_by_id(&self, id: usize, value: &RawFieldData) -> Result<()> {
                $raw_cursor::set_field_by_id(self, id, value)
            }

            fn set_fields_by_names(&self, names: &[&str], values: &[RawFieldData]) -> Result<()> {
                $raw_cursor::set_fields_by_data(self, names.iter().copied(), values)
            }

            fn set_fields_by_ids(&self, ids: &[usize], values: &[RawFieldData]) -> Result<()> {
                $raw_cursor::set_fields_by_ids(self, ids, values)
            }

            fn delete(&self) -> Result<()> {
                $raw_cursor::delete(self)
            }
        }
    };
}
pub(crate) trait RawEdgeCursor {
    fn next(&self) -> Result<bool>;
    fn goto(&self, euid: &RawEdgeUid, nearest: bool) -> Result<bool>;
    fn get_uid(&self) -> Result<RawEdgeUid>;
    fn get_src(&self) -> Result<i64>;
    fn get_dst(&self) -> Result<i64>;
    fn get_edge_id(&self) -> Result<i64>;
    fn get_temporal_id(&self) -> Result<i64>;
    fn is_valid(&self) -> bool;
    fn get_label(&self) -> Result<String>;
    fn get_label_id(&self) -> Result<u16>;
    fn get_fields_by_names(&self, names: &[&str]) -> Result<Vec<RawFieldData>>;
    fn get_field_by_name(&self, name: &str) -> Result<RawFieldData>;
    fn get_fields_by_ids(&self, ids: &[usize]) -> Result<Vec<RawFieldData>>;
    fn get_field_by_id(&self, id: usize) -> Result<RawFieldData>;
    fn get_all_fields(&self) -> Result<(Vec<String>, Vec<RawFieldData>)>;
    fn set_field_by_name(&self, name: &str, value: &RawFieldData) -> Result<()>;
    fn set_field_by_id(&self, id: usize, value: &RawFieldData) -> Result<()>;
    fn set_fields_by_names(&self, names: &[&str], values: &[RawFieldData]) -> Result<()>;
    fn set_fields_by_ids(&self, ids: &[usize], values: &[RawFieldData]) -> Result<()>;
    fn delete(&self) -> Result<()>;
}

raw_edge_cursor_impl!(RawInEdgeCursor);
raw_edge_cursor_impl!(RawOutEdgeCursor);
