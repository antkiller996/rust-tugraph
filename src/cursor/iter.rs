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

use std::marker::PhantomData;

use crate::{
    field::FieldData,
    raw::{RawEdgeIndexIterator, RawVertexIndexIterator},
    types::EdgeUid,
};

use super::{EdgeCursor, VertexCursor};

/// A iterator over vertex.
///
/// See the [`VertexCursor::into_vertices`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoVertexIter<T> {
    cursor: T,
}

impl<T> IntoVertexIter<T> {
    pub(super) fn new(cursor: T) -> IntoVertexIter<T> {
        IntoVertexIter { cursor }
    }
}

impl<T: VertexCursor> Iterator for IntoVertexIter<T> {
    type Item = (i64, String, Vec<FieldData>);

    fn next(&mut self) -> Option<(i64, String, Vec<FieldData>)> {
        if !self.cursor.is_valid() {
            return None;
        }
        let id = self.cursor.id().expect("valid iterator should get value");
        let label = self
            .cursor
            .label()
            .expect("valid iterator should get value");
        let fields = self
            .cursor
            .all_fields()
            .map(|all| all.into_iter().map(|f| f.1).collect())
            .expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some((id, label, fields))
    }
}

/// A iterator over vertex id.
///
/// See the [`VertexCursor::into_vertex_ids`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoVertexIds<T> {
    cursor: T,
}

impl<T> IntoVertexIds<T> {
    pub(super) fn new(cursor: T) -> IntoVertexIds<T> {
        IntoVertexIds { cursor }
    }
}

impl<T: VertexCursor> Iterator for IntoVertexIds<T> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        if !self.cursor.is_valid() {
            return None;
        }
        let id = self.cursor.id().expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(id)
    }
}

/// A iterator over vertex label id.
///
/// See the [`VertexCursor::into_vertex_lids`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoVertexLabelIds<T> {
    cursor: T,
}

impl<T> IntoVertexLabelIds<T> {
    pub(super) fn new(cursor: T) -> IntoVertexLabelIds<T> {
        IntoVertexLabelIds { cursor }
    }
}

impl<T: VertexCursor> Iterator for IntoVertexLabelIds<T> {
    type Item = u16;
    fn next(&mut self) -> Option<u16> {
        if !self.cursor.is_valid() {
            return None;
        }
        let lid = self.cursor.lid().expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(lid)
    }
}

/// A iterator over vertex label.
///
/// See the [`VertexCursor::into_vertex_labels`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoVertexLabels<T> {
    cursor: T,
}

impl<T> IntoVertexLabels<T> {
    pub(super) fn new(cursor: T) -> IntoVertexLabels<T> {
        IntoVertexLabels { cursor }
    }
}

impl<T: VertexCursor> Iterator for IntoVertexLabels<T> {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        if !self.cursor.is_valid() {
            return None;
        }
        let label = self
            .cursor
            .label()
            .expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(label)
    }
}

/// A iterator over vertex fields.
///
/// See the [`VertexCursor::into_vertex_fields`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoVertexFields<T> {
    cursor: T,
}

impl<T> IntoVertexFields<T> {
    pub(super) fn new(cursor: T) -> IntoVertexFields<T> {
        IntoVertexFields { cursor }
    }
}

impl<T: VertexCursor> Iterator for IntoVertexFields<T> {
    type Item = Vec<FieldData>;
    fn next(&mut self) -> Option<Vec<FieldData>> {
        if !self.cursor.is_valid() {
            return None;
        }
        let fields = self
            .cursor
            .all_fields()
            .map(|all| all.into_iter().map(|f| f.1).collect())
            .expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(fields)
    }
}

/// A iterator over edge.
///
/// See the [`EdgeCursor::into_edges`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoEdgeIter<T> {
    cursor: T,
}

impl<T> IntoEdgeIter<T> {
    pub(super) fn new(cursor: T) -> IntoEdgeIter<T> {
        IntoEdgeIter { cursor }
    }
}

impl<T: EdgeCursor> Iterator for IntoEdgeIter<T> {
    type Item = (EdgeUid, String, Vec<FieldData>);
    fn next(&mut self) -> Option<(EdgeUid, String, Vec<FieldData>)> {
        if !self.cursor.is_valid() {
            return None;
        }
        let uid = self.cursor.uid().expect("valid iterator should get value");
        let label = self
            .cursor
            .label()
            .expect("valid iterator should get value");
        let fields = self
            .cursor
            .all_fields()
            .map(|all| all.into_iter().map(|f| f.1).collect())
            .expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some((uid, label, fields))
    }
}

/// A iterator over edge uid.
///
/// See the [`EdgeCursor::into_edge_uids`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoEdgeUids<T> {
    cursor: T,
}

impl<T> IntoEdgeUids<T> {
    pub(super) fn new(cursor: T) -> IntoEdgeUids<T> {
        IntoEdgeUids { cursor }
    }
}

impl<T: EdgeCursor> Iterator for IntoEdgeUids<T> {
    type Item = EdgeUid;
    fn next(&mut self) -> Option<EdgeUid> {
        if !self.cursor.is_valid() {
            return None;
        }
        let uid = self.cursor.uid().expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(uid)
    }
}

/// A iterator over src vertex id of edge.
///
/// See the [`EdgeCursor::into_edge_srcs`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoEdgeSrcs<T> {
    cursor: T,
}

impl<T> IntoEdgeSrcs<T> {
    pub(super) fn new(cursor: T) -> IntoEdgeSrcs<T> {
        IntoEdgeSrcs { cursor }
    }
}

impl<T: EdgeCursor> Iterator for IntoEdgeSrcs<T> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        if !self.cursor.is_valid() {
            return None;
        }
        let src = self.cursor.src().expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(src)
    }
}

/// A iterator over dst vertex id of edge.
///
/// See the [`EdgeCursor::into_edge_dsts`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoEdgeDsts<T> {
    cursor: T,
}

impl<T> IntoEdgeDsts<T> {
    pub(super) fn new(cursor: T) -> IntoEdgeDsts<T> {
        IntoEdgeDsts { cursor }
    }
}

impl<T: EdgeCursor> Iterator for IntoEdgeDsts<T> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        if !self.cursor.is_valid() {
            return None;
        }
        let dst = self.cursor.dst().expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(dst)
    }
}

/// A iterator over edge id.
///
/// See the [`EdgeCursor::into_edge_eids`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoEdgeIds<T> {
    cursor: T,
}

impl<T> IntoEdgeIds<T> {
    pub(super) fn new(cursor: T) -> IntoEdgeIds<T> {
        IntoEdgeIds { cursor }
    }
}

impl<T: EdgeCursor> Iterator for IntoEdgeIds<T> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        if !self.cursor.is_valid() {
            return None;
        }
        let eid = self.cursor.eid().expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(eid)
    }
}

/// A iterator over edge temporal id.
///
/// See the [`EdgeCursor::into_edge_tids`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoEdgeTemporalIds<T> {
    cursor: T,
}

impl<T> IntoEdgeTemporalIds<T> {
    pub(super) fn new(cursor: T) -> IntoEdgeTemporalIds<T> {
        IntoEdgeTemporalIds { cursor }
    }
}

impl<T: EdgeCursor> Iterator for IntoEdgeTemporalIds<T> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        if !self.cursor.is_valid() {
            return None;
        }
        let tid = self.cursor.tid().expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(tid)
    }
}

/// A iterator over edge label id.
///
/// See the [`EdgeCursor::into_edge_lids`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoEdgeLabelIds<T> {
    cursor: T,
}

impl<T> IntoEdgeLabelIds<T> {
    pub(super) fn new(cursor: T) -> IntoEdgeLabelIds<T> {
        IntoEdgeLabelIds { cursor }
    }
}

impl<T: EdgeCursor> Iterator for IntoEdgeLabelIds<T> {
    type Item = u16;
    fn next(&mut self) -> Option<u16> {
        if !self.cursor.is_valid() {
            return None;
        }
        let lid = self.cursor.lid().expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(lid)
    }
}

/// A iterator over edge label.
///
/// See the [`EdgeCursor::into_edge_labels`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoEdgeLabels<T> {
    cursor: T,
}

impl<T> IntoEdgeLabels<T> {
    pub(super) fn new(cursor: T) -> IntoEdgeLabels<T> {
        IntoEdgeLabels { cursor }
    }
}

impl<T: EdgeCursor> Iterator for IntoEdgeLabels<T> {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        if !self.cursor.is_valid() {
            return None;
        }
        let label = self
            .cursor
            .label()
            .expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(label)
    }
}

/// A iterator over edge fields.
///
/// See the [`EdgeCursor::into_edge_fields`] for details.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoEdgeFields<T> {
    cursor: T,
}

impl<T> IntoEdgeFields<T> {
    pub(super) fn new(cursor: T) -> IntoEdgeFields<T> {
        IntoEdgeFields { cursor }
    }
}

impl<T: EdgeCursor> Iterator for IntoEdgeFields<T> {
    type Item = Vec<FieldData>;
    fn next(&mut self) -> Option<Vec<FieldData>> {
        if !self.cursor.is_valid() {
            return None;
        }
        let fields = self
            .cursor
            .all_fields()
            .map(|all| all.into_iter().map(|f| f.1).collect())
            .expect("valid iterator should get value");
        let _ = self.cursor.seek_to_next();
        Some(fields)
    }
}

/// A iterator over vertex base on index.
///
/// See the [`TxnRead::vertex_index_iter_from`] for details.
///
/// [`TxnRead::vertex_index_iter_from`]: crate::txn::TxnRead::vertex_index_iter_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct VertexIndexIter<'txn> {
    inner: RawVertexIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> VertexIndexIter<'txn> {
    pub(crate) fn new(raw_index_iter: RawVertexIndexIterator) -> VertexIndexIter<'txn> {
        VertexIndexIter {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for VertexIndexIter<'txn> {
    type Item = (i64, FieldData);
    fn next(&mut self) -> Option<(i64, FieldData)> {
        if !self.inner.is_valid() {
            return None;
        }
        let id = self
            .inner
            .get_id()
            .expect("valid iterator should get value");
        let fd = self
            .inner
            .get_index_value()
            .map(|raw| FieldData::from_raw_field_data(&raw))
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some((id, fd))
    }
}

/// A iterator over vertex id base on index.
///
/// See the [`TxnRead::vertex_index_iter_ids_from`] for details.
///
/// [`TxnRead::vertex_index_iter_ids_from`]: crate::txn::TxnRead::vertex_index_iter_ids_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct VertexIndexIds<'txn> {
    inner: RawVertexIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> VertexIndexIds<'txn> {
    pub(crate) fn new(raw_index_iter: RawVertexIndexIterator) -> VertexIndexIds<'txn> {
        VertexIndexIds {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for VertexIndexIds<'txn> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        if !self.inner.is_valid() {
            return None;
        }
        let id = self
            .inner
            .get_id()
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some(id)
    }
}

/// A iterator over vertex fields base on index.
///
/// See the [`TxnRead::vertex_index_iter_values_from`] for details.
///
/// [`TxnRead::vertex_index_iter_values_from`]: crate::txn::TxnRead::vertex_index_iter_values_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct VertexIndexValues<'txn> {
    inner: RawVertexIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> VertexIndexValues<'txn> {
    pub(crate) fn new(raw_index_iter: RawVertexIndexIterator) -> VertexIndexValues<'txn> {
        VertexIndexValues {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for VertexIndexValues<'txn> {
    type Item = FieldData;
    fn next(&mut self) -> Option<FieldData> {
        if !self.inner.is_valid() {
            return None;
        }
        let fd = self
            .inner
            .get_index_value()
            .map(|raw| FieldData::from_raw_field_data(&raw))
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some(fd)
    }
}

/// A iterator over edge base on index.
///
/// See the [`TxnRead::edge_index_iter_from`] for details.
///
/// [`TxnRead::edge_index_iter_from`]: crate::txn::TxnRead::edge_index_iter_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct EdgeIndexIter<'txn> {
    inner: RawEdgeIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> EdgeIndexIter<'txn> {
    pub(crate) fn new(raw_index_iter: RawEdgeIndexIterator) -> EdgeIndexIter<'txn> {
        EdgeIndexIter {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for EdgeIndexIter<'txn> {
    type Item = (EdgeUid, FieldData);
    fn next(&mut self) -> Option<(EdgeUid, FieldData)> {
        if !self.inner.is_valid() {
            return None;
        }
        let uid = self
            .inner
            .get_uid()
            .map(|raw| EdgeUid::from_raw(&raw))
            .expect("valid iterator should get value");
        let fd = self
            .inner
            .get_index_value()
            .map(|raw| FieldData::from_raw_field_data(&raw))
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some((uid, fd))
    }
}

/// A iterator over edge uid base on index.
///
/// See the [`TxnRead::edge_index_iter_uids_from`] for details.
///
/// [`TxnRead::edge_index_iter_uids_from`]: crate::txn::TxnRead::edge_index_iter_uids_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct EdgeIndexUids<'txn> {
    inner: RawEdgeIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> EdgeIndexUids<'txn> {
    pub(crate) fn new(raw_index_iter: RawEdgeIndexIterator) -> EdgeIndexUids<'txn> {
        EdgeIndexUids {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for EdgeIndexUids<'txn> {
    type Item = EdgeUid;
    fn next(&mut self) -> Option<EdgeUid> {
        if !self.inner.is_valid() {
            return None;
        }
        let uid = self
            .inner
            .get_uid()
            .map(|raw| EdgeUid::from_raw(&raw))
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some(uid)
    }
}

/// A iterator over src vertex id of edge base on index.
///
/// See the [`TxnRead::edge_index_iter_srcs_from`] for details.
///
/// [`TxnRead::edge_index_iter_srcs_from`]: crate::txn::TxnRead::edge_index_iter_srcs_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct EdgeIndexSrcs<'txn> {
    inner: RawEdgeIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> EdgeIndexSrcs<'txn> {
    pub(crate) fn new(raw_index_iter: RawEdgeIndexIterator) -> EdgeIndexSrcs<'txn> {
        EdgeIndexSrcs {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for EdgeIndexSrcs<'txn> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        if !self.inner.is_valid() {
            return None;
        }
        let src = self
            .inner
            .get_src()
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some(src)
    }
}

/// A iterator over dst vertex id of edge base on index.
///
/// See the [`TxnRead::edge_index_iter_dsts_from`] for details.
///
/// [`TxnRead::edge_index_iter_dsts_from`]: crate::txn::TxnRead::edge_index_iter_dsts_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct EdgeIndexDsts<'txn> {
    inner: RawEdgeIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> EdgeIndexDsts<'txn> {
    pub(crate) fn new(raw_index_iter: RawEdgeIndexIterator) -> EdgeIndexDsts<'txn> {
        EdgeIndexDsts {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for EdgeIndexDsts<'txn> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        if !self.inner.is_valid() {
            return None;
        }
        let dst = self
            .inner
            .get_dst()
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some(dst)
    }
}

/// A iterator over edge label id base on index.
///
/// See the [`TxnRead::edge_index_iter_lids_from`] for details.
///
/// [`TxnRead::edge_index_iter_lids_from`]: crate::txn::TxnRead::edge_index_iter_lids_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct EdgeIndexLabelIds<'txn> {
    inner: RawEdgeIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> EdgeIndexLabelIds<'txn> {
    pub(crate) fn new(raw_index_iter: RawEdgeIndexIterator) -> EdgeIndexLabelIds<'txn> {
        EdgeIndexLabelIds {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for EdgeIndexLabelIds<'txn> {
    type Item = u16;
    fn next(&mut self) -> Option<u16> {
        if !self.inner.is_valid() {
            return None;
        }
        let lid = self
            .inner
            .get_label_id()
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some(lid)
    }
}

/// A iterator over edge id base on index.
///
/// See the [`TxnRead::edge_index_iter_eids_from`] for details.
///
/// [`TxnRead::edge_index_iter_eids_from`]: crate::txn::TxnRead::edge_index_iter_eids_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct EdgeIndexEdgeIds<'txn> {
    inner: RawEdgeIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> EdgeIndexEdgeIds<'txn> {
    pub(crate) fn new(raw_index_iter: RawEdgeIndexIterator) -> EdgeIndexEdgeIds<'txn> {
        EdgeIndexEdgeIds {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for EdgeIndexEdgeIds<'txn> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        if !self.inner.is_valid() {
            return None;
        }
        let eid = self
            .inner
            .get_edge_id()
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some(eid)
    }
}

/// A iterator over edge field base on index.
///
/// See the [`TxnRead::edge_index_iter_values_from`] for details.
///
/// [`TxnRead::edge_index_iter_values_from`]: crate::txn::TxnRead::edge_index_iter_values_from
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct EdgeIndexValues<'txn> {
    inner: RawEdgeIndexIterator,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> EdgeIndexValues<'txn> {
    pub(crate) fn new(raw_index_iter: RawEdgeIndexIterator) -> EdgeIndexValues<'txn> {
        EdgeIndexValues {
            inner: raw_index_iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn> Iterator for EdgeIndexValues<'txn> {
    type Item = FieldData;
    fn next(&mut self) -> Option<FieldData> {
        if !self.inner.is_valid() {
            return None;
        }
        let fd = self
            .inner
            .get_index_value()
            .map(|raw| FieldData::from_raw_field_data(&raw))
            .expect("valid iterator should get value");
        let _ = self.inner.next();
        Some(fd)
    }
}
