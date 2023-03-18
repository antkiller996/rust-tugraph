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

use std::{ffi::c_char, slice};

use ffi::lgraph_api_index_spec_t;

use super::{
    RawEdgeIndexIterator, RawEdgeUid, RawFieldData, RawFieldSpec, RawInEdgeCursor, RawIndexSpec,
    RawOutEdgeCursor, RawVertexCursor, RawVertexIndexIterator,
};

use crate::{
    ffi,
    raw::ffi_util::{self, CStrLike},
    Error,
};

use std::ptr;

/// RAIIize the unsafe c bindings and Rustlize all types cross api boundary.
pub(crate) struct RawTransaction {
    inner: *mut ffi::lgraph_api_transaction_t,
}

impl Drop for RawTransaction {
    fn drop(&mut self) {
        unsafe {
            ffi_try!(ffi::lgraph_api_transaction_abort(self.inner))
                .expect("failed to drop transaction");
            ffi::lgraph_api_transaction_destroy(self.inner);
        }
    }
}

impl RawTransaction {
    pub(crate) fn from_ptr(ptr: *mut ffi::lgraph_api_transaction_t) -> Self {
        RawTransaction { inner: ptr }
    }

    pub(crate) fn as_ptr(&self) -> *const ffi::lgraph_api_transaction_t {
        self.inner
    }

    pub(crate) fn into_ptr_mut(self) -> *mut ffi::lgraph_api_transaction_t {
        self.inner
    }

    pub(crate) fn commit(self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::lgraph_api_transaction_commit(self.inner)) }
    }

    pub(crate) fn abort(self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::lgraph_api_transaction_abort(self.inner)) }
    }

    pub(crate) fn is_valid(&self) -> bool {
        unsafe { ffi::lgraph_api_transaction_is_valid(self.inner) }
    }

    pub(crate) fn is_read_only(&self) -> bool {
        unsafe { ffi::lgraph_api_transaction_is_read_only(self.inner) }
    }

    pub(crate) fn get_vertex_iterator(&self) -> Result<RawVertexCursor, Error> {
        unsafe {
            ffi_try!(ffi::lgraph_api_transaction_get_vertex_iterator(self.inner))
                .map(|ptr| unsafe { RawVertexCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_vertex_iterator_by_vid(
        &self,
        vid: i64,
        nearest: bool,
    ) -> Result<RawVertexCursor, Error> {
        unsafe {
            ffi_try!(ffi::lgraph_api_transaction_get_vertex_iterator_by_vid(
                self.inner, vid, nearest
            ))
            .map(|ptr| unsafe { RawVertexCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_out_edge_iterator_by_euid(
        &self,
        euid: &RawEdgeUid,
        nearest: bool,
    ) -> Result<RawOutEdgeCursor, Error> {
        unsafe {
            ffi_try!(ffi::lgraph_api_transaction_get_out_edge_iterator_by_euid(
                self.inner,
                euid.as_ptr(),
                nearest
            ))
            .map(|ptr| unsafe { RawOutEdgeCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_out_edge_iterator_by_src_dst_lid(
        &self,
        src: i64,
        dst: i64,
        lid: i16,
    ) -> Result<RawOutEdgeCursor, Error> {
        unsafe {
            ffi_try!(
                ffi::lgraph_api_transaction_get_out_edge_iterator_by_src_dst_lid(
                    self.inner, src, dst, lid,
                )
            )
            .map(|ptr| unsafe { RawOutEdgeCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_in_edge_iterator_by_euid(
        &self,
        euid: &RawEdgeUid,
        nearest: bool,
    ) -> Result<RawInEdgeCursor, Error> {
        unsafe {
            ffi_try!(ffi::lgraph_api_transaction_get_in_edge_iterator_by_euid(
                self.inner,
                euid.as_ptr(),
                nearest
            ))
            .map(|ptr| unsafe { RawInEdgeCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_in_edge_iterator_by_src_dst_lid(
        &self,
        src: i64,
        dst: i64,
        lid: i16,
    ) -> Result<RawInEdgeCursor, Error> {
        unsafe {
            ffi_try!(
                ffi::lgraph_api_transaction_get_in_edge_iterator_by_src_dst_lid(
                    self.inner, src, dst, lid
                )
            )
            .map(|ptr| unsafe { RawInEdgeCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_num_vertex_labels(&self) -> Result<usize, Error> {
        unsafe {
            ffi_try!(ffi::lgraph_api_transaction_get_num_vertex_labels(
                self.inner
            ))
        }
    }

    pub(crate) fn get_num_edge_labels(&self) -> Result<usize, Error> {
        unsafe { ffi_try!(ffi::lgraph_api_transaction_get_num_edge_labels(self.inner)) }
    }

    pub(crate) fn list_vertex_labels(&self) -> Result<Vec<String>, Error> {
        unsafe {
            let mut clabels: *mut *mut c_char = ptr::null_mut();
            let len = ffi_try! {
                ffi::lgraph_api_transaction_list_vertex_labels(self.inner, &mut clabels as *mut _)
            }?;
            let labels = slice::from_raw_parts(clabels, len)
                .iter()
                .map(|ptr| ffi_util::from_cstr(*ptr))
                .collect();
            ffi::lgraph_api_transaction_list_labels_destroy(clabels, len);
            Ok(labels)
        }
    }

    pub(crate) fn list_edge_labels(&self) -> Result<Vec<String>, Error> {
        unsafe {
            let mut clabels: *mut *mut c_char = ptr::null_mut();
            let len = ffi_try! {
                ffi::lgraph_api_transaction_list_edge_labels(self.inner, &mut clabels as *mut _)
            }?;
            let labels = slice::from_raw_parts(clabels, len)
                .iter()
                .map(|ptr| ffi_util::from_cstr(*ptr))
                .collect();
            ffi::lgraph_api_transaction_list_labels_destroy(clabels, len);
            Ok(labels)
        }
    }

    pub(crate) fn get_vertex_label_id<T>(&self, label: T) -> Result<usize, Error>
    where
        T: CStrLike,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_transaction_get_vertex_label_id(self.inner, clabel.as_ptr())
            }
        }
    }

    pub(crate) fn get_edge_label_id<T>(&self, label: T) -> Result<usize, Error>
    where
        T: CStrLike,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_transaction_get_edge_label_id(self.inner, clabel.as_ptr())
            }
        }
    }

    pub(crate) fn get_vertex_schema<T>(&self, label: T) -> Result<Vec<RawFieldSpec>, Error>
    where
        T: CStrLike,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let mut cfield_specs: *mut *mut ffi::lgraph_api_field_spec_t = ptr::null_mut();
            let len = ffi_try! {ffi::lgraph_api_transaction_get_vertex_schema(
                self.inner,
                clabel.as_ptr(),
                &mut cfield_specs,
            )}?;
            let schema = slice::from_raw_parts(cfield_specs, len)
                .iter()
                .map(|ptr| RawFieldSpec::from_ptr(*ptr))
                .collect();
            Ok(schema)
        }
    }

    pub(crate) fn get_edge_schema<T>(&self, label: T) -> Result<Vec<RawFieldSpec>, Error>
    where
        T: CStrLike,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let mut cfield_specs: *mut *mut ffi::lgraph_api_field_spec_t = ptr::null_mut();
            let len = ffi_try! {ffi::lgraph_api_transaction_get_edge_schema(
                self.inner,
                clabel.as_ptr(),
                &mut cfield_specs,
            )}?;
            let schema = slice::from_raw_parts(cfield_specs, len)
                .iter()
                .map(|ptr| RawFieldSpec::from_ptr(*ptr))
                .collect();
            Ok(schema)
        }
    }

    pub(crate) fn get_vertex_field_id<T: CStrLike>(
        &self,
        label_id: usize,
        field_name: T,
    ) -> Result<usize, Error> {
        unsafe {
            let cfield_name = field_name.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_transaction_get_vertex_field_id(
                self.inner,
                label_id,
                cfield_name.as_ptr(),
            )}
        }
    }

    pub(crate) fn get_vertex_field_ids<T, U>(
        &self,
        label_id: usize,
        field_names: U,
    ) -> Result<Vec<usize>, Error>
    where
        T: CStrLike + Copy,
        U: IntoIterator<Item = T>,
    {
        unsafe {
            let cfield_names: Vec<_> = field_names
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_names: Vec<_> = cfield_names.iter().map(|n| n.as_ptr()).collect();
            let mut cfield_ids: *mut usize = ptr::null_mut();
            let len = ffi_try! {
                ffi::lgraph_api_transaction_get_vertex_field_ids(
                self.inner,
                label_id,
                cfield_names.as_ptr(),
                cfield_names.len(),
                &mut cfield_ids as *mut _,
            )}?;
            let ids = slice::from_raw_parts(cfield_ids, len).to_vec();
            Ok(ids)
        }
    }

    pub(crate) fn get_edge_field_id<T>(
        &self,
        label_id: usize,
        field_name: T,
    ) -> Result<usize, Error>
    where
        T: CStrLike,
    {
        unsafe {
            let cfield_name = field_name.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_transaction_get_edge_field_id(
                self.inner,
                label_id,
                cfield_name.as_ptr(),
            )}
        }
    }

    pub(crate) fn get_edge_field_ids<T, U>(
        &self,
        label_id: usize,
        field_names: U,
    ) -> Result<Vec<usize>, Error>
    where
        T: CStrLike,
        U: IntoIterator<Item = T>,
    {
        unsafe {
            let cfield_names: Vec<_> = field_names
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_names: Vec<_> = cfield_names.iter().map(|n| n.as_ptr()).collect();
            let mut cfield_ids: *mut usize = ptr::null_mut();
            let len = ffi_try! {ffi::lgraph_api_transaction_get_vertex_field_ids(
                self.inner,
                label_id,
                cfield_names.as_ptr(),
                cfield_names.len(),
                &mut cfield_ids as *mut _,
            )}?;
            let ids = slice::from_raw_parts(cfield_ids, len).to_vec();
            Ok(ids)
        }
    }

    pub(crate) fn add_vertex_by_value_strings<T, D, E, U, S>(
        &self,
        label_name: T,
        field_names: U,
        field_value_strings: S,
    ) -> Result<i64, Error>
    where
        T: CStrLike,
        D: CStrLike + Copy,
        E: CStrLike + Copy,
        U: IntoIterator<Item = D>,
        S: IntoIterator<Item = E>,
    {
        unsafe {
            let clabel_name = label_name.into_c_string().unwrap();
            let cfield_names: Vec<_> = field_names
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_names: Vec<_> = cfield_names.iter().map(|n| n.as_ptr()).collect();
            let cfield_value_strings: Vec<_> = field_value_strings
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_value_strings: Vec<_> =
                cfield_value_strings.iter().map(|n| n.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_transaction_add_vertex_with_value_strings(
                    self.inner,
                    clabel_name.as_ptr(),
                    cfield_names.as_ptr(),
                    cfield_names.len(),
                    cfield_value_strings.as_ptr(),
                    cfield_value_strings.len()
                )
            }
        }
    }

    pub(crate) fn add_vertex_by_data<'a, T, D, U, V>(
        &self,
        label_name: T,
        field_names: U,
        field_values: V,
    ) -> Result<i64, Error>
    where
        T: CStrLike,
        D: CStrLike + Copy,
        U: IntoIterator<Item = D>,
        V: IntoIterator<Item = &'a RawFieldData>,
    {
        unsafe {
            let clabel_name = label_name.into_c_string().unwrap();
            let cfield_names: Vec<_> = field_names
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_names: Vec<_> = cfield_names.iter().map(|n| n.as_ptr()).collect();
            let cfield_values: Vec<_> = field_values.into_iter().map(|n| n.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_transaction_add_vertex_with_field_data(
                    self.inner,
                    clabel_name.as_ptr(),
                    cfield_names.as_ptr(),
                    cfield_names.len(),
                    cfield_values.as_ptr(),
                    cfield_values.len()
                )
            }
        }
    }

    pub(crate) fn add_vertex_by_ids<'a, 'b, T>(
        &self,
        label_id: usize,
        field_ids: &[usize],
        field_values: T,
    ) -> Result<i64, Error>
    where
        T: IntoIterator<Item = &'a RawFieldData>,
    {
        unsafe {
            let cfield_values: Vec<_> = field_values.into_iter().map(|n| n.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_transaction_add_vertex_with_field_data_and_id(
                    self.inner,
                    label_id,
                    field_ids.as_ptr(),
                    field_ids.len(),
                    cfield_values.as_ptr(),
                    cfield_values.len()
                )
            }
        }
    }

    pub(crate) fn add_edge_by_value_strings<T, D, E, U, V>(
        &self,
        src: i64,
        dst: i64,
        label: T,
        field_names: U,
        field_value_strings: V,
    ) -> Result<RawEdgeUid, Error>
    where
        T: CStrLike,
        D: CStrLike + Copy,
        E: CStrLike + Copy,
        U: IntoIterator<Item = D>,
        V: IntoIterator<Item = E>,
    {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_names: Vec<_> = field_names
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_names: Vec<_> = cfield_names.iter().map(|n| n.as_ptr()).collect();
            let cfield_value_strings: Vec<_> = field_value_strings
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_value_strings: Vec<_> =
                cfield_value_strings.iter().map(|n| n.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_transaction_add_edge_with_value_strings(self.inner, src, dst, clabel_name.as_ptr(), cfield_names.as_ptr(), cfield_names.len(), cfield_value_strings.as_ptr(), cfield_value_strings.len())
            }.map(|ptr| unsafe { RawEdgeUid::from_ptr(ptr) })
        }
    }

    pub(crate) fn add_edge_by_data<'a, T, D, U, V>(
        &self,
        src: i64,
        dst: i64,
        label: T,
        field_names: U,
        field_values: V,
    ) -> Result<RawEdgeUid, Error>
    where
        T: CStrLike,
        D: CStrLike + Copy,
        U: IntoIterator<Item = D>,
        V: IntoIterator<Item = &'a RawFieldData>,
    {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_names: Vec<_> = field_names
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_names: Vec<_> = cfield_names.iter().map(|n| n.as_ptr()).collect();
            let cfield_values: Vec<_> = field_values.into_iter().map(|n| n.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_transaction_add_edge_with_field_data(
                    self.inner,
                    src,
                    dst,
                    clabel_name.as_ptr(),
                    cfield_names.as_ptr(),
                    cfield_names.len(),
                    cfield_values.as_ptr(),
                    cfield_values.len()
                )
            }
            .map(|ptr| unsafe { RawEdgeUid::from_ptr(ptr) })
        }
    }

    pub(crate) fn add_edge_by_id<'a, V>(
        &self,
        src: i64,
        dst: i64,
        label_id: usize,
        field_ids: &[usize],
        field_values: V,
    ) -> Result<RawEdgeUid, Error>
    where
        V: IntoIterator<Item = &'a RawFieldData>,
    {
        unsafe {
            let cfield_values: Vec<_> = field_values.into_iter().map(|n| n.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_transaction_add_edge_with_field_data_and_id(
                    self.inner,
                    src,
                    dst,
                    label_id,
                    field_ids.as_ptr(),
                    field_ids.len(),
                    cfield_values.as_ptr(),
                    cfield_values.len()
                )
            }
            .map(|ptr| unsafe { RawEdgeUid::from_ptr(ptr) })
        }
    }

    pub(crate) fn upsert_edge_by_value_strings<T, D, E, U, S>(
        &self,
        src: i64,
        dst: i64,
        label: T,
        field_names: U,
        field_value_strings: S,
    ) -> Result<bool, Error>
    where
        T: CStrLike,
        D: CStrLike + Copy,
        E: CStrLike + Copy,
        U: IntoIterator<Item = D>,
        S: IntoIterator<Item = E>,
    {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_names: Vec<_> = field_names
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_names: Vec<_> = cfield_names.iter().map(|n| n.as_ptr()).collect();
            let cfield_value_strings: Vec<_> = field_value_strings
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_value_strings: Vec<_> =
                cfield_value_strings.iter().map(|n| n.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_transaction_upsert_edge_with_value_strings(self.inner, src, dst, clabel_name.as_ptr(), cfield_names.as_ptr(), cfield_names.len(), cfield_value_strings.as_ptr(), cfield_value_strings.len())
            }
        }
    }

    pub(crate) fn upsert_edge_by_data<'a, T, D, U, V>(
        &self,
        src: i64,
        dst: i64,
        label: T,
        field_names: U,
        field_values: V,
    ) -> Result<bool, Error>
    where
        T: CStrLike,
        D: CStrLike + Copy,
        U: IntoIterator<Item = D>,
        V: IntoIterator<Item = &'a RawFieldData>,
    {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_names: Vec<_> = field_names
                .into_iter()
                .map(|n| n.into_c_string().unwrap())
                .collect();
            let cfield_names: Vec<_> = cfield_names.iter().map(|n| n.as_ptr()).collect();
            let cfield_values: Vec<_> = field_values.into_iter().map(|n| n.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_transaction_upsert_edge_with_field_data(self.inner, src, dst, clabel_name.as_ptr(), cfield_names.as_ptr(), cfield_names.len(), cfield_values.as_ptr(), cfield_values.len())
            }
        }
    }

    pub(crate) fn upsert_edge_by_id<'a, V>(
        &self,
        src: i64,
        dst: i64,
        label_id: usize,
        field_ids: &[usize],
        field_values: V,
    ) -> Result<bool, Error>
    where
        V: IntoIterator<Item = &'a RawFieldData>,
    {
        unsafe {
            let cfield_values: Vec<_> = field_values.into_iter().map(|n| n.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_transaction_upsert_edge_with_field_data_and_id(self.inner, src, dst, label_id, field_ids.as_ptr(), field_ids.len(), cfield_values.as_ptr(), cfield_values.len())
            }
        }
    }

    pub(crate) fn list_vertex_indexes(&self) -> Result<Vec<RawIndexSpec>, Error> {
        unsafe {
            let mut cindex_specs: *mut *mut lgraph_api_index_spec_t = ptr::null_mut();
            let len = ffi_try!(ffi::lgraph_api_transaction_list_vertex_indexes(
                self.inner,
                &mut cindex_specs as *mut _
            ))?;
            let index_specs = slice::from_raw_parts(cindex_specs, len)
                .iter()
                .map(|ptr| RawIndexSpec::from_ptr(*ptr))
                .collect();
            Ok(index_specs)
        }
    }

    pub(crate) fn list_edge_indexes(&self) -> Result<Vec<RawIndexSpec>, Error> {
        unsafe {
            let mut cindex_specs: *mut *mut lgraph_api_index_spec_t = ptr::null_mut();
            let len = ffi_try!(ffi::lgraph_api_transaction_list_edge_indexes(
                self.inner,
                &mut cindex_specs as *mut _
            ))?;
            let index_specs = slice::from_raw_parts(cindex_specs, len)
                .iter()
                .map(|ptr| RawIndexSpec::from_ptr(*ptr))
                .collect();
            Ok(index_specs)
        }
    }

    pub(crate) fn get_vertex_index_iterator_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        key_start: &RawFieldData,
        key_end: &RawFieldData,
    ) -> Result<RawVertexIndexIterator, Error> {
        unsafe {
            ffi_try! {
                ffi::lgraph_api_transaction_get_vertex_index_iterator_by_id(self.inner, label_id, field_id, key_start.as_ptr(), key_end.as_ptr())
            }.map(|ptr| unsafe { RawVertexIndexIterator::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_edge_index_iterator_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        key_start: &RawFieldData,
        key_end: &RawFieldData,
    ) -> Result<RawEdgeIndexIterator, Error> {
        unsafe {
            ffi_try! {
                ffi::lgraph_api_transaction_get_edge_index_iterator_by_id(self.inner, label_id, field_id, key_start.as_ptr(), key_end.as_ptr())
            }.map(|ptr| unsafe { RawEdgeIndexIterator::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_vertex_index_iterator_by_data<T: CStrLike>(
        &self,
        label: T,
        field_name: T,
        key_start: &RawFieldData,
        key_end: &RawFieldData,
    ) -> Result<RawVertexIndexIterator, Error> {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field_name.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_transaction_get_vertex_index_iterator_by_data(self.inner, clabel_name.as_ptr(), cfield_name.as_ptr(), key_start.as_ptr(), key_end.as_ptr())
            }.map(|ptr| unsafe { RawVertexIndexIterator::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_edge_index_iterator_by_data<T: CStrLike>(
        &self,
        label: T,
        field_name: T,
        key_start: &RawFieldData,
        key_end: &RawFieldData,
    ) -> Result<RawEdgeIndexIterator, Error> {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field_name.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_transaction_get_edge_index_iterator_by_data(self.inner, clabel_name.as_ptr(), cfield_name.as_ptr(), key_start.as_ptr(), key_end.as_ptr())
            }.map(|ptr| unsafe { RawEdgeIndexIterator::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_vertex_index_iterator_by_value_string<T: CStrLike>(
        &self,
        label: T,
        field_name: T,
        key_start: T,
        key_end: T,
    ) -> Result<RawVertexIndexIterator, Error> {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field_name.into_c_string().unwrap();
            let ckey_start = key_start.into_c_string().unwrap();
            let ckey_end = key_end.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_transaction_get_vertex_index_iterator_by_value_string(self.inner, clabel_name.as_ptr(), cfield_name.as_ptr(), ckey_start.as_ptr(), ckey_end.as_ptr())
            }.map(|ptr| unsafe { RawVertexIndexIterator::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_edge_index_iterator_by_value_string<T: CStrLike>(
        &self,
        label: T,
        field_name: T,
        key_start: T,
        key_end: T,
    ) -> Result<RawEdgeIndexIterator, Error> {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field_name.into_c_string().unwrap();
            let ckey_start = key_start.into_c_string().unwrap();
            let ckey_end = key_end.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_transaction_get_edge_index_iterator_by_value_string(self.inner, clabel_name.as_ptr(), cfield_name.as_ptr(), ckey_start.as_ptr(), ckey_end.as_ptr())
            }.map(|ptr| unsafe { RawEdgeIndexIterator::from_ptr(ptr) })
        }
    }

    pub(crate) fn is_vertex_indexed<T: CStrLike>(&self, label: T, field: T) -> Result<bool, Error> {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_transaction_is_vertex_indexed(
                self.inner as *mut _,
                clabel_name.as_ptr(),
                cfield_name.as_ptr(),
            ))
        }
    }

    pub(crate) fn is_edge_indexed<T: CStrLike>(&self, label: T, field: T) -> Result<bool, Error> {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_transaction_is_edge_indexed(
                self.inner as *mut _,
                clabel_name.as_ptr(),
                cfield_name.as_ptr(),
            ))
        }
    }

    pub(crate) fn get_vertex_by_unique_index_value_string<T, U, S>(
        &self,
        label: T,
        field_name: T,
        field_value_string: T,
    ) -> Result<RawVertexCursor, Error>
    where
        T: CStrLike,
        U: CStrLike,
        S: CStrLike,
    {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field_name.into_c_string().unwrap();
            let cfield_value_string = field_value_string.into_c_string().unwrap();
            ffi_try!(
                ffi::lgraph_api_transaction_get_vertex_by_unique_index_value_string(
                    self.inner as *mut _,
                    clabel_name.as_ptr(),
                    cfield_name.as_ptr(),
                    cfield_value_string.as_ptr(),
                )
            )
            .map(|ptr| unsafe { RawVertexCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_edge_by_unique_index_value_string<T, U, S>(
        &self,
        label: T,
        field_name: U,
        field_value_string: S,
    ) -> Result<RawOutEdgeCursor, Error>
    where
        T: CStrLike,
        U: CStrLike,
        S: CStrLike,
    {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field_name.into_c_string().unwrap();
            let cfield_value_string = field_value_string.into_c_string().unwrap();
            ffi_try!(
                ffi::lgraph_api_transaction_get_edge_by_unique_index_value_string(
                    self.inner as *mut _,
                    clabel_name.as_ptr(),
                    cfield_name.as_ptr(),
                    cfield_value_string.as_ptr(),
                )
            )
            .map(|ptr| unsafe { RawOutEdgeCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_vertex_by_unique_index_by_data<T, U>(
        &self,
        label: T,
        field_name: U,
        field_data: &RawFieldData,
    ) -> Result<RawVertexCursor, Error>
    where
        T: CStrLike,
        U: CStrLike,
    {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field_name.into_c_string().unwrap();
            ffi_try!(
                ffi::lgraph_api_transaction_get_vertex_by_unique_index_with_data(
                    self.inner as *mut _,
                    clabel_name.as_ptr(),
                    cfield_name.as_ptr(),
                    field_data.as_ptr(),
                )
            )
            .map(|ptr| unsafe { RawVertexCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_edge_by_unique_index_by_data<T, U>(
        &self,
        label: T,
        field_name: U,
        field_data: &RawFieldData,
    ) -> Result<RawOutEdgeCursor, Error>
    where
        T: CStrLike,
        U: CStrLike,
    {
        unsafe {
            let clabel_name = label.into_c_string().unwrap();
            let cfield_name = field_name.into_c_string().unwrap();
            ffi_try!(
                ffi::lgraph_api_transaction_get_edge_by_unique_index_with_data(
                    self.inner as *mut _,
                    clabel_name.as_ptr(),
                    cfield_name.as_ptr(),
                    field_data.as_ptr(),
                )
            )
            .map(|ptr| unsafe { RawOutEdgeCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_vertex_by_unique_index_id(
        &self,
        label_id: usize,
        field_id: usize,
        field_value: &RawFieldData,
    ) -> Result<RawVertexCursor, Error> {
        unsafe {
            ffi_try! {ffi::lgraph_api_transaction_get_vertex_by_unique_index_with_id(
                self.inner,
                label_id,
                field_id,
                field_value.as_ptr(),
            )}
            .map(|ptr| unsafe { RawVertexCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_edge_by_unique_index_id(
        &self,
        label_id: usize,
        field_id: usize,
        field_value: &RawFieldData,
    ) -> Result<RawOutEdgeCursor, Error> {
        unsafe {
            ffi_try! {ffi::lgraph_api_transaction_get_edge_by_unique_index_with_id(
                self.inner,
                label_id,
                field_id,
                field_value.as_ptr(),
            )}
            .map(|ptr| unsafe { RawOutEdgeCursor::from_ptr(ptr) })
        }
    }

    pub(crate) fn get_num_vertices(&self) -> Result<usize, Error> {
        unsafe { ffi_try!(ffi::lgraph_api_transaction_get_num_vertices(self.inner)) }
    }

    pub(crate) fn get_vertex_primary_field<T: CStrLike>(&self, label: T) -> Result<String, Error> {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_transaction_get_vertex_primary_field(
                self.inner,
                clabel.as_ptr()
            ))
            .map(|cstr| ffi_util::to_rust_string(cstr))
        }
    }
}
