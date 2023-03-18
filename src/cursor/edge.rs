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
    raw::{RawEdgeCursor, RawInEdgeCursor, RawOutEdgeCursor},
    types::EdgeUid,
    Result,
};

use super::iter::{
    IntoEdgeDsts, IntoEdgeFields, IntoEdgeIds, IntoEdgeIter, IntoEdgeLabelIds, IntoEdgeLabels,
    IntoEdgeSrcs, IntoEdgeTemporalIds, IntoEdgeUids,
};

trait AsRawEdgeCursor {
    type RawEdgeCur: RawEdgeCursor;
    fn as_raw(&self) -> &Self::RawEdgeCur;
}

/// A cursor that allows you to seek edge with given [`EdgeUid`](primary key),
/// and to read the edge fields after the cursor.
pub trait EdgeCursor {
    /// Get uid of the current edge.
    fn uid(&self) -> Result<EdgeUid>;

    /// Get src vertex id of the current edge.
    fn src(&self) -> Result<i64>;

    /// Get dst vertex id of the current edge.
    fn dst(&self) -> Result<i64>;

    /// Get edge id of the current edge.
    fn eid(&self) -> Result<i64>;

    /// Get temporal id of the current edge.
    fn tid(&self) -> Result<i64>;

    /// Get label of the current edge.
    fn label(&self) -> Result<String>;

    /// Get label id of the current edge.
    fn lid(&self) -> Result<u16>;

    /// Query if this cursor is valid.
    ///
    /// The following operations invalidates a EdgeIterator:
    /// - Calling [`EdgeCursor::seek`] with the id of a non-existing edge.  
    /// - Calling [`EdgeCursor::seek_to_next`] on the last edge.  
    /// - Calling [`EdgeCursorMut::delete`] on the last edge.
    fn is_valid(&self) -> bool;

    /// Seek to next edge.
    fn seek_to_next(&mut self) -> Result<Option<&mut Self>>;

    /// Seek to the edge with euid.
    ///
    /// If there is no edge with exactly the same `euid`, and nearest==true,
    /// seek to the next edge with uid>=`euid`, otherwise invalidate the iterator.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    fn seek(&mut self, euid: &EdgeUid, nearest: bool) -> Result<&mut Self>;

    /// Convert cursor to iterator over edge.
    ///
    /// **Note**: Iterate over the returned `EdgeIter` will
    /// make the cursor move forward.
    fn into_edges(self) -> IntoEdgeIter<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over edge uid.
    ///
    /// **Note**: Iterate over the returned `EdgeUids` will
    /// make the cursor move forward.
    fn into_edge_uids(self) -> IntoEdgeUids<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over src vertex id of edge.
    ///
    /// **Note**: Iterate over the returned `EdgeSrcs` will
    /// make the cursor move forward.
    fn into_edge_srcs(self) -> IntoEdgeSrcs<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over dst vertex id of edge.
    ///
    /// **Note**: Iterate over the returned `EdgeDsts` will
    /// make the cursor move forward.
    fn into_edge_dsts(self) -> IntoEdgeDsts<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over edge id.
    ///
    /// **Note**: Iterate over the returned `EdgeIds` will
    /// make the cursor move forward.
    fn into_edge_eids(self) -> IntoEdgeIds<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over edge temporal id.
    ///
    /// **Note**: Iterate over the returned `EdgeTemporalIds` will
    /// make the cursor move forward.   
    fn into_edge_tids(self) -> IntoEdgeTemporalIds<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over edge label id.
    ///
    /// **Note**: Iterate over the returned `EdgeLabelIds` will
    /// make the cursor move forward.  
    fn into_edge_lids(self) -> IntoEdgeLabelIds<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over edge label.
    ///
    /// **Note**: Iterate over the returned `EdgeLabels` will
    /// make the cursor move forward.  
    fn into_edge_labels(self) -> IntoEdgeLabels<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over edge fields.
    ///
    /// **Note**: Iterate over the returned `EdgeFields` will
    /// make the cursor move forward.
    fn into_edge_fields(self) -> IntoEdgeFields<Self>
    where
        Self: Sized;

    /// Convert cursor to iterator over edge starting from `euid`.
    ///
    /// **Note**: Iterate over the returned `EdgeIter` will
    /// make the cursor move forward.
    fn into_edges_from(self, euid: &EdgeUid, nearest: bool) -> Result<IntoEdgeIter<Self>>
    where
        Self: Sized;

    /// Get field value with given name of the current edge.
    fn field(&self, name: &str) -> Result<FieldData>;

    /// Get fields values with given names of the current edge.
    fn fields(&self, names: &[&str]) -> Result<Vec<FieldData>>;

    /// Get field value with given field id of the current edge.
    fn field_by_id(&self, id: usize) -> Result<FieldData>;

    /// Get fields values with given field ids of the current edge.
    fn fields_by_ids(&self, ids: &[usize]) -> Result<Vec<FieldData>>;

    /// Get all fields return name and its value.
    fn all_fields(&self) -> Result<Vec<(String, FieldData)>>;
}

impl<T> EdgeCursor for T
where
    T: AsRawEdgeCursor,
{
    fn uid(&self) -> Result<EdgeUid> {
        self.as_raw().get_uid().map(|raw| EdgeUid::from_raw(&raw))
    }
    fn src(&self) -> Result<i64> {
        self.as_raw().get_src()
    }
    fn dst(&self) -> Result<i64> {
        self.as_raw().get_dst()
    }
    fn eid(&self) -> Result<i64> {
        self.as_raw().get_edge_id()
    }
    fn tid(&self) -> Result<i64> {
        self.as_raw().get_temporal_id()
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
    fn seek(&mut self, euid: &EdgeUid, nearest: bool) -> Result<&mut Self> {
        debug_assert!(self.as_raw().goto(&euid.as_raw(), nearest)?);
        Ok(self)
    }
    fn into_edges(self) -> IntoEdgeIter<Self>
    where
        Self: Sized,
    {
        IntoEdgeIter::new(self)
    }
    fn into_edge_uids(self) -> IntoEdgeUids<Self>
    where
        Self: Sized,
    {
        IntoEdgeUids::new(self)
    }

    fn into_edge_srcs(self) -> IntoEdgeSrcs<Self>
    where
        Self: Sized,
    {
        IntoEdgeSrcs::new(self)
    }
    fn into_edge_dsts(self) -> IntoEdgeDsts<Self>
    where
        Self: Sized,
    {
        IntoEdgeDsts::new(self)
    }
    fn into_edge_eids(self) -> IntoEdgeIds<Self>
    where
        Self: Sized,
    {
        IntoEdgeIds::new(self)
    }
    fn into_edge_tids(self) -> IntoEdgeTemporalIds<Self>
    where
        Self: Sized,
    {
        IntoEdgeTemporalIds::new(self)
    }
    fn into_edge_lids(self) -> IntoEdgeLabelIds<Self>
    where
        Self: Sized,
    {
        IntoEdgeLabelIds::new(self)
    }
    fn into_edge_labels(self) -> IntoEdgeLabels<Self>
    where
        Self: Sized,
    {
        IntoEdgeLabels::new(self)
    }
    fn into_edge_fields(self) -> IntoEdgeFields<Self>
    where
        Self: Sized,
    {
        IntoEdgeFields::new(self)
    }
    fn into_edges_from(mut self, euid: &EdgeUid, nearest: bool) -> Result<IntoEdgeIter<Self>>
    where
        Self: Sized,
    {
        self.seek(euid, nearest)?;
        Ok(IntoEdgeIter::new(self))
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
}

/// A out edge cursor for navigating out edges of a vertex.
///
/// See the [`VertexCursor::out_edge_cursor`] for details.
///
/// [`VertexCursor::out_edge_cursor`]: crate::cursor::VertexCursor::out_edge_cursor
pub struct OutEdgeCur<'v> {
    inner: RawOutEdgeCursor,
    // lifetime 'v from VertexCur
    _marker: PhantomData<&'v ()>,
}

impl<'v> OutEdgeCur<'v> {
    pub(crate) fn new(raw_cursor: RawOutEdgeCursor) -> OutEdgeCur<'v> {
        OutEdgeCur {
            inner: raw_cursor,
            _marker: PhantomData,
        }
    }
}

impl<'v> AsRawEdgeCursor for OutEdgeCur<'v> {
    type RawEdgeCur = RawOutEdgeCursor;
    fn as_raw(&self) -> &Self::RawEdgeCur {
        &self.inner
    }
}

/// A in edge cursor for navigating in edges of a vertex.
///
/// See the [`VertexCursor::in_edge_cursor`] for details.
///
/// [`VertexCursor::in_edge_cursor`]: crate::cursor::VertexCursor::in_edge_cursor
pub struct InEdgeCur<'v> {
    inner: RawInEdgeCursor,
    // lifetime 'v from VertexCur
    _marker: PhantomData<&'v ()>,
}

impl<'v> InEdgeCur<'v> {
    pub(super) fn new(raw_cursor: RawInEdgeCursor) -> InEdgeCur<'v> {
        InEdgeCur {
            inner: raw_cursor,
            _marker: PhantomData,
        }
    }
}

impl<'v> AsRawEdgeCursor for InEdgeCur<'v> {
    type RawEdgeCur = RawInEdgeCursor;
    fn as_raw(&self) -> &Self::RawEdgeCur {
        &self.inner
    }
}

/// A cursor that allows you to write the edge fields after the cursor.
pub trait EdgeCursorMut {
    /// Set field value with given name of the current edge.
    fn set_field(&self, name: &str, value: &FieldData) -> Result<()>;

    /// Set field value with given field id of the current edge.
    fn set_field_by_id(&self, id: usize, value: &FieldData) -> Result<()>;

    /// Set fields values with given names of the current edge.
    fn set_fields(&self, names: &[&str], values: &[FieldData]) -> Result<()>;

    /// Set fields values with given field ids of the current edge.
    fn set_fields_by_ids(&self, ids: &[usize], values: &[FieldData]) -> Result<()>;

    /// Delete the current edge.
    fn delete(&self) -> Result<()>;
}

macro_rules! edge_cursor_mut_impl {
    ($cursor_mut:ident) => {
        impl<'txn> EdgeCursorMut for $cursor_mut<'txn> {
            fn set_field(
                &self,
                name: &str,
                value: &$crate::field::FieldData,
            ) -> $crate::Result<()> {
                self.as_raw()
                    .set_field_by_name(name, &value.as_raw_field_data())
            }

            fn set_field_by_id(
                &self,
                id: usize,
                value: &$crate::field::FieldData,
            ) -> $crate::Result<()> {
                self.as_raw()
                    .set_field_by_id(id, &value.as_raw_field_data())
            }

            fn set_fields(
                &self,
                names: &[&str],
                values: &[$crate::field::FieldData],
            ) -> $crate::Result<()> {
                let values: Vec<_> = values.iter().map(|fd| fd.as_raw_field_data()).collect();
                self.as_raw().set_fields_by_names(names, &values)
            }

            fn set_fields_by_ids(
                &self,
                ids: &[usize],
                values: &[$crate::field::FieldData],
            ) -> $crate::Result<()> {
                let values: Vec<_> = values.iter().map(|fd| fd.as_raw_field_data()).collect();
                self.as_raw().set_fields_by_ids(ids, &values)
            }

            // return number of deleted vertices and edges
            fn delete(&self) -> $crate::Result<()> {
                self.as_raw().delete()
            }
        }
    };
}

/// A out edge cursor for navigating in edges of a vertex, which allows you modifying
/// fields.
///
/// See the [`VertexCursor::out_edge_cursor`] for details.
///
/// [`VertexCursor::out_edge_cursor`]: crate::cursor::VertexCursor::out_edge_cursor
pub struct OutEdgeCurMut<'v> {
    inner: RawOutEdgeCursor,
    _marker: PhantomData<&'v mut ()>,
}

impl<'v> OutEdgeCurMut<'v> {
    pub(crate) fn new(raw_cursor: RawOutEdgeCursor) -> OutEdgeCurMut<'v> {
        OutEdgeCurMut {
            inner: raw_cursor,
            _marker: PhantomData,
        }
    }
}

impl<'v> AsRawEdgeCursor for OutEdgeCurMut<'v> {
    type RawEdgeCur = RawOutEdgeCursor;
    fn as_raw(&self) -> &Self::RawEdgeCur {
        &self.inner
    }
}
edge_cursor_mut_impl!(OutEdgeCurMut);

/// A in edge cursor for navigating in edges of a vertex, which allows you modifying
/// fields.
///
/// See the [`VertexCursor::in_edge_cursor`] for details.
///
/// [`VertexCursor::in_edge_cursor`]: crate::cursor::VertexCursor::in_edge_cursor
pub struct InEdgeCurMut<'v> {
    inner: RawInEdgeCursor,
    _marker: PhantomData<&'v mut ()>,
}

impl<'v> InEdgeCurMut<'v> {
    pub(super) fn new(raw_cursor: RawInEdgeCursor) -> InEdgeCurMut<'v> {
        InEdgeCurMut {
            inner: raw_cursor,
            _marker: PhantomData,
        }
    }
}

impl<'v> AsRawEdgeCursor for InEdgeCurMut<'v> {
    type RawEdgeCur = RawInEdgeCursor;
    fn as_raw(&self) -> &Self::RawEdgeCur {
        &self.inner
    }
}

edge_cursor_mut_impl!(InEdgeCurMut);
