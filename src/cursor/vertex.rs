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

use crate::{field::FieldData, raw::RawVertexCursor, Result};

use super::{
    iter::{IntoVertexFields, IntoVertexIds, IntoVertexIter, IntoVertexLabelIds, IntoVertexLabels},
    InEdgeCur, InEdgeCurMut, OutEdgeCur, OutEdgeCurMut,
};

trait AsRawVertexCursor {
    fn as_raw(&self) -> &RawVertexCursor;
}

/// A cursor that allows you to seek vertex with given vid(primary key),
/// and to read the vertex fields after the cursor.
pub trait VertexCursor {
    /// Get current vertex id.
    fn id(&self) -> Result<i64>;

    /// Get current vertex label.
    fn label(&self) -> Result<String>;

    /// Get label id of current vertex label.
    fn lid(&self) -> Result<u16>;

    /// Query if this cursor is valid.
    ///
    /// The following operations invalidates a VertexIterator:
    /// - Calling [`VertexCursor::seek`] with the id of a non-existing vertex.  
    /// - Calling [`VertexCursor::seek_to_next`] on the last vertex.  
    /// - Calling [`VertexCursorMut::delete`] on the last vertex.
    fn is_valid(&self) -> bool;

    /// Seek to next vertex.
    fn seek_to_next(&mut self) -> Result<Option<&mut Self>>;

    /// Seek to the vertex with id src.
    ///
    /// If there is no vertex with exactly the same vid, and nearest==true,
    /// seek to the next vertex with id>=vid, otherwise invalidate the iterator.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    fn seek(&mut self, vid: i64, nearest: bool) -> Result<&mut Self>;

    /// Convert cursor to iterator over vertex.
    ///
    /// **Note**: Iterate over the returned `VertexIter` will
    /// make the cursor move forward.
    fn into_vertices(self) -> IntoVertexIter<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over vertex id.
    ///
    /// **Note**: Iterate over the returned `VertexIds` will
    /// make the cursor move forward.
    fn into_vertex_ids(self) -> IntoVertexIds<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over vertex label.
    ///
    /// **Note**: Iterate over the returned `VertexLabels` will
    /// make the cursor move forward.
    fn into_vertex_labels(self) -> IntoVertexLabels<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over vertex label id.
    ///
    /// **Note**: Iterate over the returned `VertexLabelIds` will
    /// make the cursor move forward.
    fn into_vertex_lids(self) -> IntoVertexLabelIds<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over vertex fields.
    ///
    /// **Note**: Iterate over the returned `VertexFields` will
    /// make the cursor move forward.
    fn into_vertex_fields(self) -> IntoVertexFields<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over vertex starting from `vid`.
    ///
    /// **Note**: Iterate over the returned `VertexIter` will
    /// make the cursor move forward.
    fn into_vertices_from(self, vid: i64, nearest: bool) -> Result<IntoVertexIter<Self>>
    where
        Self: Sized;

    /// Get out edge cursor of the current vertex.
    fn out_edge_cursor(&mut self) -> Result<OutEdgeCur<'_>>;

    /// Get in edge cursor of the current vertex.
    fn in_edge_cursor(&mut self) -> Result<InEdgeCur<'_>>;

    /// Get field value with given name of the current vertex.
    fn field(&self, name: &str) -> Result<FieldData>;

    /// Get fields values with given names of the current vertex.
    fn fields(&self, names: &[&str]) -> Result<Vec<FieldData>>;

    /// Get field value with given field id of the current vertex.
    fn field_by_id(&self, id: usize) -> Result<FieldData>;

    /// Get fields values with given field ids of the current vertex.
    fn fields_by_ids(&self, ids: &[usize]) -> Result<Vec<FieldData>>;

    /// Get all fields return name and its value.
    fn all_fields(&self) -> Result<Vec<(String, FieldData)>>;

    /// List src vids of edges associated to the vertex after this cursor.
    ///
    /// The first of return tuple shows whether more to go.
    fn associated_edges_src_vids(&self, limit: usize) -> Result<(bool, Vec<i64>)>;

    /// List dst vids of edges associated to the vertex after this cursor.
    ///
    /// The first of return tuple shows whether more to go.
    fn associated_edges_dst_vids(&self, limit: usize) -> Result<(bool, Vec<i64>)>;

    /// Get number of in edges associated to the vertex after this cursor.
    ///
    /// The first of return tuple shows whether more to go.
    fn num_in_edges(&self, limit: usize) -> Result<(bool, usize)>;

    /// Get number of out edges associated to the vertex after this cursor.
    ///
    /// The first of return tuple shows whether more to go.
    fn num_out_edges(&self, limit: usize) -> Result<(bool, usize)>;
}

impl<T> VertexCursor for T
where
    T: AsRawVertexCursor,
{
    fn id(&self) -> Result<i64> {
        self.as_raw().get_id()
    }

    fn label(&self) -> Result<String> {
        self.as_raw().get_label()
    }

    fn lid(&self) -> Result<u16> {
        self.as_raw().get_label_id()
    }

    fn is_valid(&self) -> bool {
        self.as_raw().is_valid()
    }

    fn seek_to_next(&mut self) -> Result<Option<&mut Self>> {
        if self.as_raw().next()? {
            Ok(Some(self))
        } else {
            Ok(None)
        }
    }

    fn seek(&mut self, vid: i64, nearest: bool) -> Result<&mut Self> {
        let ret = self.as_raw().goto(vid, nearest)?;
        debug_assert!(ret);
        Ok(self)
    }

    fn into_vertices(self) -> IntoVertexIter<Self>
    where
        Self: Sized,
    {
        IntoVertexIter::new(self)
    }

    fn into_vertex_ids(self) -> IntoVertexIds<Self>
    where
        Self: Sized,
    {
        IntoVertexIds::new(self)
    }

    fn into_vertex_labels(self) -> IntoVertexLabels<Self>
    where
        Self: Sized,
    {
        IntoVertexLabels::new(self)
    }

    fn into_vertex_lids(self) -> IntoVertexLabelIds<Self>
    where
        Self: Sized,
    {
        IntoVertexLabelIds::new(self)
    }

    fn into_vertex_fields(self) -> IntoVertexFields<Self>
    where
        Self: Sized,
    {
        IntoVertexFields::new(self)
    }

    fn into_vertices_from(mut self, vid: i64, nearest: bool) -> Result<IntoVertexIter<Self>>
    where
        Self: Sized,
    {
        self.seek(vid, nearest)?;
        Ok(IntoVertexIter::new(self))
    }

    fn out_edge_cursor(&mut self) -> Result<OutEdgeCur<'_>> {
        self.as_raw().get_out_edge_cursor().map(OutEdgeCur::new)
    }

    fn in_edge_cursor(&mut self) -> Result<InEdgeCur<'_>> {
        self.as_raw().get_in_edge_cursor().map(InEdgeCur::new)
    }

    fn field(&self, name: &str) -> Result<FieldData> {
        self.as_raw()
            .get_field_by_name(name)
            .map(|fd| FieldData::from_raw_field_data(&fd))
    }

    fn fields(&self, names: &[&str]) -> Result<Vec<FieldData>> {
        self.as_raw()
            .get_fields_by_names(names)
            .map(|v| v.iter().map(FieldData::from_raw_field_data).collect())
    }

    fn field_by_id(&self, id: usize) -> Result<FieldData> {
        self.as_raw()
            .get_field_by_id(id)
            .map(|fd| FieldData::from_raw_field_data(&fd))
    }

    fn fields_by_ids(&self, ids: &[usize]) -> Result<Vec<FieldData>> {
        self.as_raw()
            .get_fields_by_ids(ids)
            .map(|v| v.iter().map(FieldData::from_raw_field_data).collect())
    }

    fn all_fields(&self) -> Result<Vec<(String, FieldData)>> {
        self.as_raw().get_all_fields().map(|(names, datas)| {
            names
                .into_iter()
                .zip(datas.into_iter())
                .map(|(name, data)| (name, FieldData::from_raw_field_data(&data)))
                .collect()
        })
    }

    // return list src vids of edges associated to the vertex after this cursor
    // the first of return tuple shows whether more to go
    fn associated_edges_src_vids(&self, limit: usize) -> Result<(bool, Vec<i64>)> {
        self.as_raw().list_src_vids(limit)
    }

    // return list src vids of edges associated to the vertex after this cursor
    // the first of return tuple shows whether more to go
    fn associated_edges_dst_vids(&self, limit: usize) -> Result<(bool, Vec<i64>)> {
        self.as_raw().list_dst_vids(limit)
    }

    // return number of in edges associated to the vertex after this cursor
    // the first of return tuple shows whether more to go
    fn num_in_edges(&self, limit: usize) -> Result<(bool, usize)> {
        self.as_raw().get_num_in_edges(limit)
    }

    // return number of out edges associated to the vertex after this cursor
    // the first of return tuple shows whether more to go
    fn num_out_edges(&self, limit: usize) -> Result<(bool, usize)> {
        self.as_raw().get_num_out_edges(limit)
    }
}

/// A vertex cursor for navigating vertices within a graph.
///
/// See the [`TxnRead::vertex_cur`] for details.
///
/// [`TxnRead::vertex_cur`]: crate::txn::TxnRead::vertex_cur
pub struct VertexCur<'txn> {
    inner: RawVertexCursor,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> VertexCur<'txn> {
    pub(crate) fn new(raw_cursor: RawVertexCursor) -> VertexCur<'txn> {
        VertexCur {
            inner: raw_cursor,
            _marker: PhantomData,
        }
    }
}

impl<'txn> AsRawVertexCursor for VertexCur<'txn> {
    fn as_raw(&self) -> &RawVertexCursor {
        &self.inner
    }
}

/// A cursor that allows you to write the vertex fields after the cursor.
pub trait VertexCursorMut {
    /// Get out edge cursor of the current vertex that allows you modifying edge.
    fn out_edge_cursor_mut(&mut self) -> Result<OutEdgeCurMut<'_>>;

    /// Get in edge cursor of the current vertex that allows you modifying edge.
    fn int_edge_cursor_mut(&mut self) -> Result<InEdgeCurMut<'_>>;

    /// Set field value with given name of the current vertex.
    fn set_field(&self, name: &str, value: &FieldData) -> Result<()>;

    /// Set field value with given field id of the current vertex.
    fn set_field_by_id(&self, id: usize, value: &FieldData) -> Result<()>;

    /// Set fields values with given names of the current vertex.
    fn set_fields(&self, names: &[&str], values: &[FieldData]) -> Result<()>;

    /// Set fields values with given field ids of the current vertex.
    fn set_fields_by_ids(&self, ids: &[usize], values: &[FieldData]) -> Result<()>;

    /// Delete the current vertex and return number of deleted vertices and edges
    fn delete(&self) -> Result<(usize, usize)>;
}

impl<'txn> VertexCursorMut for VertexCurMut<'txn> {
    fn out_edge_cursor_mut(&mut self) -> Result<OutEdgeCurMut<'_>> {
        self.as_raw().get_out_edge_cursor().map(OutEdgeCurMut::new)
    }

    fn int_edge_cursor_mut(&mut self) -> Result<InEdgeCurMut<'_>> {
        self.as_raw().get_in_edge_cursor().map(InEdgeCurMut::new)
    }

    fn set_field(&self, name: &str, value: &FieldData) -> Result<()> {
        self.as_raw()
            .set_field_by_name(name, &value.as_raw_field_data())
    }

    fn set_field_by_id(&self, id: usize, value: &FieldData) -> Result<()> {
        self.as_raw()
            .set_field_by_id(id, &value.as_raw_field_data())
    }

    fn set_fields(&self, names: &[&str], values: &[FieldData]) -> Result<()> {
        let values: Vec<_> = values.iter().map(|fd| fd.as_raw_field_data()).collect();
        self.as_raw()
            .set_fields_by_data(names.iter().copied(), &values)
    }

    fn set_fields_by_ids(&self, ids: &[usize], values: &[FieldData]) -> Result<()> {
        let values: Vec<_> = values.iter().map(|fd| fd.as_raw_field_data()).collect();
        self.as_raw().set_fields_by_ids(ids, &values)
    }

    // return number of deleted vertices and edges
    fn delete(&self) -> Result<(usize, usize)> {
        self.as_raw().delete()
    }
}

/// A vertex cursor for navigating vertex within a graph, which allows you modifying
/// fields.
///
/// See the [`TxnWrite::vertex_cur_mut`] for details.
///
/// [`TxnWrite::vertex_cur_mut`]: crate::txn::TxnWrite::vertex_cur_mut
pub struct VertexCurMut<'txn> {
    inner: RawVertexCursor,
    _marker: PhantomData<&'txn mut ()>,
}

impl<'txn> VertexCurMut<'txn> {
    pub(crate) fn new(raw_cursor: RawVertexCursor) -> VertexCurMut<'txn> {
        VertexCurMut {
            inner: raw_cursor,
            _marker: PhantomData,
        }
    }
}

impl<'txn> AsRawVertexCursor for VertexCurMut<'txn> {
    fn as_raw(&self) -> &RawVertexCursor {
        &self.inner
    }
}
