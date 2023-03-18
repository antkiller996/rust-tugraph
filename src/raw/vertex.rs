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

use super::{RawEdgeUid, RawFieldData, RawInEdgeCursor, RawOutEdgeCursor};
use std::{ffi::CStr, fmt};

macro_rules! raw_cursor_vertex_ext_rustlize {
    (
        $cursor_name:ident,
        $cursor_get_id:ident
        $(
        ,
        $cursor_goto:ident,
        $cursor_get_out_edge_cursor:ident,
        $cursor_get_out_edge_cursor_by_euid:ident,
        $cursor_get_in_edge_cursor:ident,
        $cursor_get_in_edge_cursor_by_euid:ident,
        $cursor_list_src_vids:ident,
        $cursor_list_dst_vids:ident,
        $cursor_get_num_in_edges:ident,
        $cursor_get_num_out_edges:ident,
        $cursor_delete:ident,
        )?
        $(,)?
    ) => {
        impl $cursor_name {
            pub(crate) fn get_id(&self) -> Result<i64, $crate::Error> {
                unsafe {
                    ffi_try! { $crate::ffi::$cursor_get_id(self.inner) }
                }
            }

            $(
            pub(crate) fn goto(
                &self,
                vid: i64,
                nearest: bool,
            ) -> Result<bool, $crate::Error> {
                unsafe {
                    ffi_try! {$crate::ffi::$cursor_goto(
                        self.inner, vid, nearest
                    )}
                }
            }

            pub(crate) fn get_out_edge_cursor(&self) -> Result<RawOutEdgeCursor, $crate::Error> {
                unsafe {
                    ffi_try! {
                        $crate::ffi::$cursor_get_out_edge_cursor(self.inner)
                    }
                    .map(|ptr| unsafe { RawOutEdgeCursor::from_ptr(ptr) })
                }
            }
            pub(crate) fn get_out_edge_cursor_by_euid(
                &self,
                euid: &RawEdgeUid,
                nearest: bool,
            ) -> Result<RawOutEdgeCursor, $crate::Error> {
                unsafe {
                    ffi_try! {
                        $crate::ffi::$cursor_get_out_edge_cursor_by_euid(self.inner, euid.as_ptr(), nearest)
                    }
                    .map(|ptr| unsafe { RawOutEdgeCursor::from_ptr(ptr) })
                }
            }

            pub(crate) fn get_in_edge_cursor(&self) -> Result<RawInEdgeCursor, $crate::Error> {
                unsafe {
                    ffi_try! {$crate::ffi::$cursor_get_in_edge_cursor(self.inner)}
                        .map(|ptr| unsafe { RawInEdgeCursor::from_ptr(ptr) })
                }
            }

            pub(crate) fn get_in_edge_cursor_by_euid(
                &self,
                euid: &RawEdgeUid,
                nearest: bool,
            ) -> Result<RawInEdgeCursor, $crate::Error> {
                unsafe {
                    ffi_try! {
                        $crate::ffi::$cursor_get_in_edge_cursor_by_euid(self.inner, euid.as_ptr(), nearest)
                    }.map(|ptr| unsafe { RawInEdgeCursor::from_ptr(ptr) })
                }
            }

            pub(crate) fn list_src_vids(&self, n_limit: usize) -> Result<(bool, Vec<i64>), $crate::Error> {
                unsafe {
                    let mut more_to_go = false;
                    let mut cvids: *mut i64 = std::ptr::null_mut();
                    let len = ffi_try! {
                        $crate::ffi::$cursor_list_src_vids(
                            self.inner,
                            n_limit,
                            &mut more_to_go as *mut _,
                            &mut cvids as *mut _,
                        )
                    }?;
                    let vids = std::slice::from_raw_parts(cvids, len).to_vec();
                    $crate::ffi::lgraph_api_vertex_iterator_list_vids_destroy(cvids);
                    Ok((more_to_go, vids))
                }
            }

            pub(crate) fn list_dst_vids(&self, n_limit: usize) -> Result<(bool, Vec<i64>), $crate::Error> {
                unsafe {
                    let mut more_to_go = false;
                    let mut cvids: *mut i64 = std::ptr::null_mut();
                    let len = ffi_try! {
                        $crate::ffi::$cursor_list_dst_vids(
                            self.inner,
                            n_limit,
                            &mut more_to_go as *mut _,
                            &mut cvids as *mut _,
                        )
                    }?;
                    let vids = std::slice::from_raw_parts(cvids, len).to_vec();
                    $crate::ffi::lgraph_api_vertex_iterator_list_vids_destroy(cvids);
                    Ok((more_to_go, vids))
                }
            }

            pub(crate) fn get_num_in_edges(&self, n_limit: usize) -> Result<(bool, usize), $crate::Error> {
                unsafe {
                    let mut more_to_go = false;
                    ffi_try! {
                        $crate::ffi::$cursor_get_num_in_edges(
                            self.inner,
                            n_limit,
                            &mut more_to_go as *mut _,
                        )
                    }
                    .map(|num| (more_to_go, num))
                }
            }

            pub(crate) fn get_num_out_edges(&self, n_limit: usize) -> Result<(bool, usize), $crate::Error> {
                unsafe {
                    let mut more_to_go = false;
                    ffi_try! {
                        $crate::ffi::$cursor_get_num_out_edges(
                            self.inner,
                            n_limit,
                            &mut more_to_go as *mut _,
                        )
                    }
                    .map(|num| (more_to_go, num))
                }
            }

            pub(crate) fn delete(&self) -> Result<(usize, usize), $crate::Error> {
                unsafe {
                    let (mut n_in_edges, mut n_out_edges) = (0_usize, 0_usize);
                    ffi_try! {
                        $crate::ffi::$cursor_delete(
                            self.inner,
                            &mut n_in_edges as *mut _,
                            &mut n_out_edges as *mut _,
                        )
                    }?;
                    Ok((n_in_edges, n_out_edges))
                }
            }
            )?
        }
    };
}

macro_rules! raw_vertex_cursor_rustlize {
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
        $cursor_get_id:ident,
        $cursor_goto:ident,
        $cursor_get_out_edge_cursor:ident,
        $cursor_get_out_edge_cursor_by_euid:ident,
        $cursor_get_in_edge_cursor:ident,
        $cursor_get_in_edge_cursor_by_euid:ident,
        $cursor_list_src_vids:ident,
        $cursor_list_dst_vids:ident,
        $cursor_get_num_in_edges:ident,
        $cursor_get_num_out_edges:ident,
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
        raw_cursor_vertex_ext_rustlize!(
            $cursor_name,
            $cursor_get_id,
            $cursor_goto,
            $cursor_get_out_edge_cursor,
            $cursor_get_out_edge_cursor_by_euid,
            $cursor_get_in_edge_cursor,
            $cursor_get_in_edge_cursor_by_euid,
            $cursor_list_src_vids,
            $cursor_list_dst_vids,
            $cursor_get_num_in_edges,
            $cursor_get_num_out_edges,
            $cursor_delete,
        );
    };
}

raw_vertex_cursor_rustlize!(
    RawVertexCursor,
    lgraph_api_vertex_iterator_t,
    lgraph_api_vertex_iterator_close,
    lgraph_api_vertex_iterator_destroy,
    lgraph_api_vertex_iterator_is_valid,
    lgraph_api_vertex_iterator_next,
    lgraph_api_vertex_iterator_get_label,
    lgraph_api_vertex_iterator_get_label_id,
    lgraph_api_vertex_iterator_get_fields_by_names,
    lgraph_api_vertex_iterator_get_field_by_name,
    lgraph_api_vertex_iterator_get_fields_by_ids,
    lgraph_api_vertex_iterator_get_field_by_id,
    lgraph_api_vertex_iterator_get_all_fields,
    lgraph_api_vertex_iterator_set_field_by_name,
    lgraph_api_vertex_iterator_set_field_by_id,
    lgraph_api_vertex_iterator_set_fields_by_value_strings,
    lgraph_api_vertex_iterator_set_fields_by_data,
    lgraph_api_vertex_iterator_set_fields_by_ids,
    lgraph_api_vertex_iterator_get_id,
    lgraph_api_vertex_iterator_goto,
    lgraph_api_vertex_iterator_get_out_edge_iterator,
    lgraph_api_vertex_iterator_get_out_edge_iterator_by_euid,
    lgraph_api_vertex_iterator_get_in_edge_iterator,
    lgraph_api_vertex_iterator_get_in_edge_iterator_by_euid,
    lgraph_api_vertex_iterator_list_src_vids,
    lgraph_api_vertex_iterator_list_dst_vids,
    lgraph_api_vertex_iterator_get_num_in_edges,
    lgraph_api_vertex_iterator_get_num_out_edges,
    lgraph_api_vertex_iterator_delete,
);

impl fmt::Display for RawVertexCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            let ptr = crate::ffi::lgraph_api_vertex_iterator_to_string(self.inner);
            f.write_str(CStr::from_ptr(ptr).to_str().unwrap())?;
            Ok(())
        }
    }
}

impl fmt::Debug for RawVertexCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

macro_rules! raw_vertex_index_cursor_rustlize {
    (
        $cursor_name:ident,
        $cursor_ctype:ident,
        $cursor_close:ident,
        $cursor_destroy:ident,
        $cursor_is_valid:ident,
        $cursor_next:ident,
        $cursor_get_id:ident,
    ) => {
        raw_core_cursor_rustlize!(
            $cursor_name,
            $cursor_ctype,
            $cursor_close,
            $cursor_destroy,
            $cursor_is_valid,
            $cursor_next
        );

        impl $cursor_name {
            pub(crate) fn get_index_value(&self) -> Result<RawFieldData, $crate::Error> {
                unsafe {
                    ffi_try! {
                        $crate::ffi::lgraph_api_vertex_index_iterator_get_index_value(self.inner)
                    }
                    .map(|ptr| RawFieldData::from_ptr(ptr))
                }
            }
        }

        raw_cursor_vertex_ext_rustlize!($cursor_name, $cursor_get_id);
    };
}

raw_vertex_index_cursor_rustlize!(
    RawVertexIndexIterator,
    lgraph_api_vertex_index_iterator_t,
    lgraph_api_vertex_index_iterator_close,
    lgraph_api_vertex_index_iterator_destroy,
    lgraph_api_vertex_index_iterator_is_valid,
    lgraph_api_vertex_index_iterator_next,
    lgraph_api_vertex_index_iterator_get_vid,
);
