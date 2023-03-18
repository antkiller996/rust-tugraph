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

use std::{ffi::c_char, ptr, slice};

use crate::{
    ffi,
    raw::ffi_util::{to_rust_string, CStrLike},
    Error,
};

use super::{ffi_util, txn::RawTransaction, RawEdgeUid, RawFieldData, RawFieldSpec};

pub(crate) struct RawGraphDB {
    inner: *mut ffi::lgraph_api_graph_db_t,
}

impl Drop for RawGraphDB {
    fn drop(&mut self) {
        unsafe {
            ffi_try!(ffi::lgraph_api_graph_db_close(self.inner)).expect("failed to close db");
            ffi::lgraph_api_graph_db_destroy(self.inner);
        }
    }
}

impl RawGraphDB {
    ///
    /// # Safety
    ///
    /// The `ptr` passed in must be a valid and non-null pointer
    /// created by libtugraph_sys::lgraph_api_galaxy_open_graph
    pub(crate) unsafe fn from_ptr(ptr: *mut ffi::lgraph_api_graph_db_t) -> Self {
        RawGraphDB { inner: ptr }
    }

    pub(crate) fn create_read_txn(&self) -> Result<RawTransaction, Error> {
        unsafe {
            ffi_try! {
                ffi::lgraph_api_graph_db_create_read_txn(self.inner)
            }
            .map(RawTransaction::from_ptr)
        }
    }

    pub(crate) fn create_write_txn(&self, optimistic: bool) -> Result<RawTransaction, Error> {
        unsafe {
            ffi_try! {
                ffi::lgraph_api_graph_db_create_write_txn(self.inner, optimistic)
            }
            .map(RawTransaction::from_ptr)
        }
    }

    ///
    /// # Safety
    ///
    /// The `txn` must be a read-only transaction created by [`RawGraphDB::create_read_txn`]
    pub(crate) unsafe fn fork_txn(&self, txn: &RawTransaction) -> Result<RawTransaction, Error> {
        unsafe {
            ffi_try!(ffi::lgraph_api_graph_db_fork_txn(
                self.inner,
                txn.as_ptr() as *mut _
            ))
            .map(RawTransaction::from_ptr)
        }
    }

    pub(crate) fn flush(&self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::lgraph_api_graph_db_flush(self.inner)) }
    }

    pub(crate) fn drop_all_data(&self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::lgraph_api_graph_db_drop_all_data(self.inner)) }
    }

    pub(crate) fn drop_all_vertex(&self) -> Result<(), Error> {
        unsafe {
            ffi_try! { ffi::lgraph_api_graph_db_drop_all_vertex(self.inner) }
        }
    }

    pub(crate) fn estimate_num_vertices(&self) -> Result<usize, Error> {
        unsafe {
            ffi_try! { ffi::lgraph_api_graph_db_estimate_num_vertices(self.inner) }
        }
    }

    pub(crate) fn add_vertex_label<'a, T, U>(
        &self,
        label: T,
        field_specs: U,
        primary_field: T,
    ) -> Result<bool, Error>
    where
        T: CStrLike,
        U: IntoIterator<Item = &'a RawFieldSpec>,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cprimary_field = primary_field.into_c_string().unwrap();
            let cfield_specs: Vec<_> = field_specs.into_iter().map(|f| f.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_graph_db_add_vertex_label(
                self.inner,
                clabel.as_ptr(),
                cfield_specs.as_ptr(),
                cfield_specs.len(),
                cprimary_field.as_ptr(),
            )}
        }
    }

    pub(crate) fn delete_vertex_label<T: CStrLike>(
        &self,
        label: T,
    ) -> Result<(bool, usize), Error> {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let mut n_modified = 0_usize;
            ffi_try! {
                ffi::lgraph_api_graph_db_delete_vertex_label(self.inner, clabel.as_ptr(), &mut n_modified as *mut _)
            }.map(|b| (b, n_modified))
        }
    }

    pub(crate) fn alter_vertex_label_del_fields<T, D, U>(
        &self,
        label: T,
        del_fields: U,
    ) -> Result<(bool, usize), Error>
    where
        T: CStrLike,
        D: CStrLike + Copy,
        U: IntoIterator<Item = D>,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let del_fields: Vec<_> = del_fields
                .into_iter()
                .map(|f| f.into_c_string().unwrap())
                .collect();
            let del_fields: Vec<_> = del_fields.iter().map(|f| f.as_ptr()).collect();
            let mut n_modified = 0_usize;
            ffi_try! {ffi::lgraph_api_graph_db_alter_vertex_label_del_fields(
                self.inner,
                clabel.as_ptr(),
                del_fields.as_ptr(),
                del_fields.len(),
                &mut n_modified,
            )}
            .map(|b| (b, n_modified))
        }
    }

    pub(crate) fn alter_vertex_label_add_fields<'a, 'b, T, U, D>(
        &self,
        label: T,
        add_fields: U,
        default_values: D,
    ) -> Result<(bool, usize), Error>
    where
        T: CStrLike,
        U: IntoIterator<Item = &'a RawFieldSpec>,
        D: IntoIterator<Item = &'b RawFieldData>,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cadd_fields: Vec<_> = add_fields.into_iter().map(|f| f.as_ptr()).collect();
            let cdefault_values: Vec<_> = default_values.into_iter().map(|v| v.as_ptr()).collect();
            let mut n_modified = 0_usize;
            ffi_try! {ffi::lgraph_api_graph_db_alter_vertex_label_add_fields(
                self.inner,
                clabel.as_ptr(),
                cadd_fields.as_ptr(),
                cadd_fields.len(),
                cdefault_values.as_ptr(),
                cdefault_values.len(),
                &mut n_modified,
            )}
            .map(|b| (b, n_modified))
        }
    }

    pub(crate) fn alter_vertex_label_mod_fields<'a, T, U>(
        &self,
        label: T,
        mod_fields: U,
    ) -> Result<(bool, usize), Error>
    where
        T: CStrLike,
        U: IntoIterator<Item = &'a RawFieldSpec>,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cmod_fields: Vec<_> = mod_fields.into_iter().map(|f| f.as_ptr()).collect();
            let mut n_modified = 0_usize;
            ffi_try! {ffi::lgraph_api_graph_db_alter_vertex_label_mod_fields(
                self.inner,
                clabel.as_ptr(),
                cmod_fields.as_ptr(),
                cmod_fields.len(),
                &mut n_modified,
            )}
            .map(|b| (b, n_modified))
        }
    }

    pub(crate) fn add_edge_label<'a, T, D, U, C1, C2, C>(
        &self,
        label: T,
        field_specs: U,
        temporal_field: D,
        edge_constraints: C,
    ) -> Result<bool, Error>
    where
        T: CStrLike,
        D: CStrLike,
        C1: CStrLike + Copy,
        C2: CStrLike + Copy,
        U: IntoIterator<Item = &'a RawFieldSpec>,
        C: IntoIterator<Item = (C1, C2)>,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cfield_specs: Vec<_> = field_specs.into_iter().map(|f| f.as_ptr()).collect();
            let ctemporal_field = temporal_field.into_c_string().unwrap();
            let (first_edge_constraints, second_edge_constraints): (Vec<_>, Vec<_>) =
                edge_constraints
                    .into_iter()
                    .map(|(e1, e2)| (e1.into_c_string().unwrap(), e2.into_c_string().unwrap()))
                    .unzip();
            let cfirst_edge_constraints: Vec<_> =
                first_edge_constraints.iter().map(|s| s.as_ptr()).collect();
            let csecond_edge_constraints: Vec<_> =
                second_edge_constraints.iter().map(|s| s.as_ptr()).collect();
            ffi_try! {ffi::lgraph_api_graph_db_add_edge_label(
                self.inner,
                clabel.as_ptr(),
                cfield_specs.as_ptr(),
                cfield_specs.len(),
                ctemporal_field.as_ptr(),
                cfirst_edge_constraints.as_ptr(),
                csecond_edge_constraints.as_ptr(),
                cfirst_edge_constraints.len(),
            )}
        }
    }

    pub(crate) fn delete_edge_label<T: CStrLike>(&self, label: T) -> Result<(bool, usize), Error> {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let mut n_modified = 0_usize;
            ffi_try!(ffi::lgraph_api_graph_db_delete_edge_label(
                self.inner,
                clabel.as_ptr(),
                &mut n_modified as *mut _,
            ))
            .map(|b| (b, n_modified))
        }
    }

    pub(crate) fn alter_label_mod_edge_constraints<T, D1, D2, U>(
        &self,
        label: T,
        constraints: U,
    ) -> Result<bool, Error>
    where
        T: CStrLike,
        D1: CStrLike + Copy,
        D2: CStrLike + Copy,
        U: IntoIterator<Item = (D1, D2)>,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();

            let (first_edge_constraints, second_edge_constraints): (Vec<_>, Vec<_>) = constraints
                .into_iter()
                .map(|(e1, e2)| (e1.into_c_string().unwrap(), e2.into_c_string().unwrap()))
                .unzip();
            let cfirst_edge_constraints: Vec<_> =
                first_edge_constraints.iter().map(|s| s.as_ptr()).collect();
            let csecond_edge_constraints: Vec<_> =
                second_edge_constraints.iter().map(|s| s.as_ptr()).collect();
            ffi_try! {ffi::lgraph_api_graph_db_alter_label_mod_edge_constraints(
                self.inner,
                clabel.as_ptr(),
                cfirst_edge_constraints.as_ptr(),
                csecond_edge_constraints.as_ptr(),
                cfirst_edge_constraints.len(),
            )}
        }
    }

    pub(crate) fn alter_edge_label_del_fields<T, D, U>(
        &self,
        label: T,
        del_fields: U,
    ) -> Result<(bool, usize), Error>
    where
        T: CStrLike,
        D: CStrLike + Copy,
        U: IntoIterator<Item = D>,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let del_fields: Vec<_> = del_fields
                .into_iter()
                .map(|f| f.into_c_string().unwrap())
                .collect();
            let del_fields: Vec<_> = del_fields.iter().map(|f| f.as_ptr()).collect();
            let mut n_modified = 0_usize;
            ffi_try! {ffi::lgraph_api_graph_db_alter_edge_label_del_fields(
                self.inner,
                clabel.as_ptr(),
                del_fields.as_ptr(),
                del_fields.len(),
                &mut n_modified,
            )}
            .map(|b| (b, n_modified))
        }
    }

    pub(crate) fn alter_edge_label_add_fields<'a, 'b, T, U, D>(
        &self,
        label: T,
        add_fields: U,
        default_values: D,
    ) -> Result<(bool, usize), Error>
    where
        T: CStrLike,
        U: IntoIterator<Item = &'a RawFieldSpec>,
        D: IntoIterator<Item = &'b RawFieldData>,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cadd_fields: Vec<_> = add_fields.into_iter().map(|f| f.as_ptr()).collect();
            let cdefault_values: Vec<_> = default_values.into_iter().map(|v| v.as_ptr()).collect();
            let mut n_modified = 0_usize;
            ffi_try! {ffi::lgraph_api_graph_db_alter_edge_label_add_fields(
                self.inner,
                clabel.as_ptr(),
                cadd_fields.as_ptr(),
                cadd_fields.len(),
                cdefault_values.as_ptr(),
                cdefault_values.len(),
                &mut n_modified,
            )}
            .map(|b| (b, n_modified))
        }
    }

    pub(crate) fn alter_edge_label_mod_fields<'a, T, U>(
        &self,
        label: T,
        mod_fields: U,
    ) -> Result<(bool, usize), Error>
    where
        T: CStrLike,
        U: IntoIterator<Item = &'a RawFieldSpec>,
    {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cmod_fields: Vec<_> = mod_fields.into_iter().map(|f| f.as_ptr()).collect();
            let mut n_modified = 0_usize;
            ffi_try! {ffi::lgraph_api_graph_db_alter_edge_label_mod_fields(
                self.inner,
                clabel.as_ptr(),
                cmod_fields.as_ptr(),
                cmod_fields.len(),
                &mut n_modified,
            )}
            .map(|b| (b, n_modified))
        }
    }

    pub(crate) fn add_vertex_index<T: CStrLike>(
        &self,
        label: T,
        field: T,
        is_unique: bool,
    ) -> Result<bool, Error> {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cfield = field.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_graph_db_add_vertex_index(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr(),
                is_unique,
            ))
        }
    }

    pub(crate) fn add_edge_index<T: CStrLike>(
        &self,
        label: T,
        field: T,
        is_unique: bool,
    ) -> Result<bool, Error> {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cfield = field.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_graph_db_add_edge_index(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr(),
                is_unique,
            ))
        }
    }

    pub(crate) fn is_vertex_indexed<T: CStrLike>(&self, label: T, field: T) -> Result<bool, Error> {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cfield = field.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_graph_db_is_vertex_indexed(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr()
            ))
        }
    }

    pub(crate) fn is_edge_indexed<T: CStrLike>(&self, label: T, field: T) -> Result<bool, Error> {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cfield = field.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_graph_db_is_edge_indexed(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr()
            ))
        }
    }

    pub(crate) fn delete_vertex_index<T: CStrLike>(
        &self,
        label: T,
        field: T,
    ) -> Result<bool, Error> {
        let clabel = label.into_c_string().unwrap();
        let cfield = field.into_c_string().unwrap();
        unsafe {
            ffi_try!(ffi::lgraph_api_graph_db_delete_vertex_index(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr(),
            ))
        }
    }

    pub(crate) fn delete_edge_index<T: CStrLike>(&self, label: T, field: T) -> Result<bool, Error> {
        let clabel = label.into_c_string().unwrap();
        let cfield = field.into_c_string().unwrap();
        unsafe {
            ffi_try!(ffi::lgraph_api_graph_db_delete_edge_index(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr(),
            ))
        }
    }

    pub(crate) fn get_description(&self) -> Result<String, Error> {
        unsafe {
            ffi_try!(ffi::lgraph_api_graph_db_get_description(self.inner))
                .map(|cstr| ffi_util::to_rust_string(cstr))
        }
    }

    pub(crate) fn get_max_size(&self) -> Result<usize, Error> {
        unsafe { ffi_try!(ffi::lgraph_api_graph_db_get_max_size(self.inner)) }
    }

    pub(crate) fn add_vertex_full_text_index<T: CStrLike>(
        &self,
        label: T,
        field: T,
    ) -> Result<bool, Error> {
        let clabel = label.into_c_string().unwrap();
        let cfield = field.into_c_string().unwrap();
        unsafe {
            ffi_try!(ffi::lgraph_api_graph_db_add_vertex_full_text_index(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr(),
            ))
        }
    }

    pub(crate) fn add_edge_full_text_index<T: CStrLike>(
        &self,
        label: T,
        field: T,
    ) -> Result<bool, Error> {
        let clabel = label.into_c_string().unwrap();
        let cfield = field.into_c_string().unwrap();
        unsafe {
            ffi_try!(ffi::lgraph_api_graph_db_add_edge_full_text_index(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr(),
            ))
        }
    }

    pub(crate) fn delete_vertex_full_text_index<T: CStrLike>(
        &self,
        label: T,
        field: T,
    ) -> Result<bool, Error> {
        let clabel = label.into_c_string().unwrap();
        let cfield = field.into_c_string().unwrap();
        unsafe {
            ffi_try!(ffi::lgraph_api_graph_db_delete_vertex_full_text_index(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr(),
            ))
        }
    }

    pub(crate) fn delete_edge_full_text_index<T: CStrLike>(
        &self,
        label: T,
        field: T,
    ) -> Result<bool, Error> {
        let clabel = label.into_c_string().unwrap();
        let cfield = field.into_c_string().unwrap();
        unsafe {
            ffi_try!(ffi::lgraph_api_graph_db_delete_edge_full_text_index(
                self.inner,
                clabel.as_ptr(),
                cfield.as_ptr(),
            ))
        }
    }

    pub(crate) fn rebuild_full_text_index<T, D, V, E>(
        &self,
        vertex_labels: V,
        edge_labels: E,
    ) -> Result<(), Error>
    where
        T: CStrLike,
        D: CStrLike,
        V: IntoIterator<Item = T>,
        E: IntoIterator<Item = D>,
    {
        unsafe {
            let cvertex_labels: Vec<_> = vertex_labels
                .into_iter()
                .map(|l| l.into_c_string().unwrap())
                .collect();
            let cedge_labels: Vec<_> = edge_labels
                .into_iter()
                .map(|l| l.into_c_string().unwrap())
                .collect();
            let cvertex_labels: Vec<_> = cvertex_labels.iter().map(|s| s.as_ptr()).collect();
            let cedge_labels: Vec<_> = cedge_labels.iter().map(|s| s.as_ptr()).collect();
            ffi_try! {
                ffi::lgraph_api_graph_db_rebuild_full_text_index(
                self.inner,
                cvertex_labels.as_ptr(),
                cvertex_labels.len(),
                cedge_labels.as_ptr(),
                cedge_labels.len(),
            )}
        }
    }

    pub(crate) fn list_full_text_indexes(&self) -> Result<Vec<(bool, String, String)>, Error> {
        unsafe {
            let mut cis_vertex: *mut bool = ptr::null_mut();
            let mut clabel_names: *mut *mut c_char = ptr::null_mut();
            let mut cproperty_names: *mut *mut c_char = ptr::null_mut();
            let len = ffi_try! {
                ffi::lgraph_api_graph_db_list_full_text_indexes(
                    self.inner,
                    &mut cis_vertex as *mut _,
                    &mut clabel_names as *mut _,
                    &mut cproperty_names as *mut _,
                )
            }?;
            let is_vertex = slice::from_raw_parts(cis_vertex, len).to_vec();
            let label_names: Vec<_> = slice::from_raw_parts(clabel_names, len)
                .iter()
                .map(|s| to_rust_string(*s))
                .collect();
            let property_names: Vec<_> = slice::from_raw_parts(cproperty_names, len)
                .iter()
                .map(|s| to_rust_string(*s))
                .collect();
            let indexes: Vec<_> = is_vertex
                .into_iter()
                .zip(label_names.into_iter())
                .zip(property_names.into_iter())
                .map(|((is, l), p)| (is, l, p))
                .collect();
            Ok(indexes)
        }
    }

    pub(crate) fn query_vertex_by_full_text_index<T: CStrLike>(
        &self,
        label: T,
        query: T,
        topn: i32,
    ) -> Result<Vec<(i64, f32)>, Error> {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cquery = query.into_c_string().unwrap();
            let mut cvids: *mut i64 = ptr::null_mut();
            let mut cscores: *mut f32 = ptr::null_mut();
            let len = ffi_try! {
                ffi::lgraph_api_graph_db_query_vertex_by_full_text_index(
                self.inner,
                clabel.as_ptr(),
                cquery.as_ptr(),
                topn,
                &mut cvids as *mut _ ,
                &mut cscores as *mut _,
            )}?;
            let vids = slice::from_raw_parts(cvids, len).to_vec();
            let scores = slice::from_raw_parts(cscores, len).to_vec();
            let indexes: Vec<_> = vids.into_iter().zip(scores.into_iter()).collect();
            Ok(indexes)
        }
    }

    pub(crate) fn query_edge_by_full_text_index<T: CStrLike>(
        &self,
        label: T,
        query: T,
        topn: i32,
    ) -> Result<Vec<(RawEdgeUid, f32)>, Error> {
        unsafe {
            let clabel = label.into_c_string().unwrap();
            let cquery = query.into_c_string().unwrap();
            let mut ceuids: *mut *mut ffi::lgraph_api_edge_uid_t = ptr::null_mut();
            let mut cscores: *mut f32 = ptr::null_mut();
            let len = ffi_try! {
                ffi::lgraph_api_graph_db_query_edge_by_full_text_index(
                self.inner,
                clabel.as_ptr(),
                cquery.as_ptr(),
                topn,
                &mut ceuids as *mut _ ,
                &mut cscores as *mut _,
            )}?;
            let euids: Vec<_> = slice::from_raw_parts(ceuids, len)
                .iter()
                .map(|e| RawEdgeUid::from_ptr(*e))
                .collect();
            let scores = slice::from_raw_parts(cscores, len).to_vec();
            let indexes: Vec<_> = euids.into_iter().zip(scores.into_iter()).collect();
            Ok(indexes)
        }
    }
}
