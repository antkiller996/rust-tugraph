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

use std::{
    collections::HashSet,
    sync::{atomic::AtomicUsize, Barrier},
    thread,
};
use tugraph::{
    cursor::{EdgeCursor, VertexCursor},
    field::{FieldData, FieldSpec, FieldType},
    txn::{TxnRead, TxnWrite},
};

mod common;

#[test]
fn test_concurrent_vertex_add() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let graph = galaxy.open_graph("default", false).unwrap();
    let added = graph
        .add_vertex_label(
            "v",
            &[FieldSpec {
                name: "id".into(),
                ty: FieldType::Int64,
                optional: false,
            }],
            "id",
        )
        .unwrap();
    assert!(added);

    const NTHREADS: usize = 3;
    let (barrier, n_success, n_fail) = (
        Barrier::new(NTHREADS),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    );
    thread::scope(|s| {
        let jhs: Vec<_> = (0..NTHREADS)
            .map(|id| {
                let (graph, barrier, n_success, n_fail) = (&graph, &barrier, &n_success, &n_fail);
                s.spawn(move || {
                    let mut rw_txn = graph.create_rw_txn(true).unwrap();
                    rw_txn
                        .add_vertex("v", &["id"], &[FieldData::Int64(id as i64)])
                        .expect("add vertex to optimistic txn before commiting should be ok");
                    barrier.wait();
                    if rw_txn.commit().is_ok() {
                        n_success.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        n_fail.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                })
            })
            .collect();
        jhs.into_iter().for_each(|j| j.join().unwrap());
    });
    let ro_txn = graph.create_ro_txn().unwrap();
    assert_eq!(n_success.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        n_fail.load(std::sync::atomic::Ordering::Relaxed),
        NTHREADS - 1
    );
    assert_eq!(ro_txn.num_vertices().unwrap(), 1);
}

#[test]
fn test_rw_txn_co_existence() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let graph = galaxy.open_graph("default", false).unwrap();
    let num_rw_txn = AtomicUsize::new(0);

    thread::scope(|s| {
        (0..10).for_each(|_| {
            s.spawn(|| {
                let _rw_txn = graph.create_rw_txn(false);
                // only one  non-optimistic read-write transaction can be active simutanously.
                assert_eq!(
                    num_rw_txn.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    0
                );
                num_rw_txn.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            });
        })
    });

    thread::scope(|s| {
        (0..10).for_each(|_| {
            s.spawn(|| {
                let rw_txn = graph.create_rw_txn(true);
                // multiple optimistic read-writes transaction can be active simutanously.
                assert!(rw_txn.is_ok());
            });
        })
    });
}

#[test]
fn test_ro_txn_co_existence() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let graph = galaxy.open_graph("default", true).unwrap();
    thread::scope(|s| {
        (0..10).for_each(|_| {
            s.spawn(|| {
                let ro_txn = graph.create_ro_txn();
                // multiple optimistic read-writes transaction can be active simutanously.
                assert!(ro_txn.is_ok());
            });
        })
    });
}

#[test]
fn test_txn_nest() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let graph = galaxy.open_graph("default", false).unwrap();
    {
        let _ro_txn = graph.create_ro_txn().unwrap();
        assert!(graph.create_ro_txn().is_err());
        assert!(graph.create_rw_txn(false).is_err());
    }
    {
        let _rw_txn = graph.create_rw_txn(false).unwrap();
        assert!(graph.create_ro_txn().is_err());
        assert!(graph.create_rw_txn(false).is_err());
    }
    {
        let _rw_txn = graph.create_rw_txn(false).unwrap();
        assert!(graph.create_ro_txn().is_err());
        assert!(graph.create_rw_txn(true).is_err());
    }
    {
        let _rw_txn = graph.create_rw_txn(true).unwrap();
        assert!(graph.create_ro_txn().is_err());
        assert!(graph.create_rw_txn(false).is_err());
    }
    {
        let _rw_txn = graph.create_rw_txn(true).unwrap();
        assert!(graph.create_ro_txn().is_err());
        assert!(graph.create_rw_txn(true).is_err());
    }
}

#[test]
fn test_txn_readwrite() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let graph = galaxy.open_graph("default", false).unwrap();
    {
        // begin read-only transaction
        let ro_txn = graph.create_ro_txn().unwrap();
        assert!(ro_txn.is_valid());
        assert!(ro_txn.is_read_only());
        assert_eq!(ro_txn.num_edge_labels().unwrap(), 0);
        assert_eq!(ro_txn.num_vertex_labels().unwrap(), 0);
        assert!(ro_txn.all_vertex_labels().unwrap().is_empty());
        assert!(ro_txn.all_edge_labels().unwrap().is_empty());
    } // end read-only transaction
    assert!(graph
        .add_vertex_label(
            "src",
            &[FieldSpec {
                name: "id".into(),
                ty: FieldType::String,
                optional: false,
            }],
            "id",
        )
        .unwrap());
    assert!(graph
        .add_vertex_label(
            "dst",
            &[FieldSpec {
                name: "id".into(),
                ty: FieldType::String,
                optional: false,
            }],
            "id",
        )
        .unwrap());
    assert!(graph
        .add_edge_label(
            "edge",
            &[FieldSpec {
                name: "tid".into(),
                ty: FieldType::String,
                optional: false,
            }],
            "",
            [("src", "dst")],
        )
        .unwrap());

    // test add vertex/edge by name
    const NUM_ADDED_BY_NAME: usize = 4;
    // test add vertex/edge by id
    const NUM_ADDED_BY_ID: usize = 4;
    let all_edges = {
        // begin read-write transaction to add vertex/edges
        let mut rw_txn = graph.create_rw_txn(false).unwrap();
        assert!(rw_txn.is_valid());
        assert!(!rw_txn.is_read_only());

        let mut edges = vec![];
        (0..NUM_ADDED_BY_NAME).for_each(|i| {
            let src = rw_txn
                .add_vertex("src", &["id"], &[FieldData::String(format!("src_{i}"))])
                .unwrap();
            let dst = rw_txn
                .add_vertex("dst", &["id"], &[FieldData::String(format!("dst_{i}"))])
                .unwrap();
            let euid = rw_txn
                .add_edge(
                    src,
                    dst,
                    "edge",
                    &["tid"],
                    &[FieldData::String(format!("edge_{src}_{dst}"))],
                )
                .unwrap();
            assert!(euid.src == src && euid.dst == dst);
            edges.push((src, dst));
        });

        let src_lid = rw_txn.vertex_label_id("src").unwrap();
        let src_id_fid = rw_txn.vertex_field_id(src_lid, "id").unwrap();
        let dst_lid = rw_txn.vertex_label_id("dst").unwrap();
        let dst_id_fid = rw_txn.vertex_field_id(dst_lid, "id").unwrap();
        let edge_lid = rw_txn.edge_label_id("edge").unwrap();
        let edge_tid_fid = rw_txn.edge_field_id(edge_lid, "tid").unwrap();
        (0..NUM_ADDED_BY_ID).map(|i| i + 100).for_each(|i| {
            let src = rw_txn
                .add_vertex_by_id(
                    src_lid,
                    &[src_id_fid],
                    &[FieldData::String(format!("src_{i}"))],
                )
                .unwrap();
            let dst = rw_txn
                .add_vertex_by_id(
                    dst_lid,
                    &[dst_id_fid],
                    &[FieldData::String(format!("dst_{i}"))],
                )
                .unwrap();
            let euid = rw_txn
                .add_edge_by_id(
                    src,
                    dst,
                    edge_lid,
                    &[edge_tid_fid],
                    &[FieldData::String(format!("edge_{src}_{dst}"))],
                )
                .unwrap();
            assert!(euid.src == src && euid.dst == dst);
            edges.push((src, dst));
        });

        rw_txn.commit().unwrap();
        edges
    }; // end read-write transaction

    {
        // begin read-only transaction
        let ro_txn = graph.create_ro_txn().unwrap();
        assert_eq!(ro_txn.num_edge_labels().unwrap(), 1);
        assert_eq!(ro_txn.num_vertex_labels().unwrap(), 2);
        assert_eq!(
            ro_txn
                .all_vertex_labels()
                .unwrap()
                .into_iter()
                .collect::<HashSet<String>>(),
            HashSet::from_iter(["src".into(), "dst".into()])
        );
        assert_eq!(ro_txn.all_edge_labels().unwrap(), vec!["edge"]);

        // check vertex/edge schema
        let src_fields = ro_txn.vertex_schema("src").unwrap();
        assert_eq!(src_fields.len(), 1);
        assert_eq!(src_fields.first().unwrap().name, "id");
        let edge_fields = ro_txn.edge_schema("edge").unwrap();
        assert_eq!(edge_fields.len(), 1);
        assert_eq!(edge_fields.first().unwrap().name, "tid");

        // check data
        let all_vertex_fields: HashSet<_> = ro_txn
            .vertex_cur()
            .unwrap()
            .into_vertex_fields()
            .map(|fd| {
                assert_eq!(fd.len(), 1);
                match fd.first().unwrap() {
                    FieldData::String(v) => v.clone(),
                    _ => panic!("field data should be string type"),
                }
            })
            .collect();
        assert_eq!(
            all_vertex_fields,
            HashSet::from_iter(
                (0..NUM_ADDED_BY_ID)
                    .chain((0..NUM_ADDED_BY_NAME).map(|i| i + 100))
                    .flat_map(|id| [format!("src_{id}"), format!("dst_{id}")])
            )
        );

        let all_edge_fields: HashSet<_> = all_edges
            .iter()
            .flat_map(|e| {
                ro_txn
                    .vertex_cur()
                    .unwrap()
                    .seek(e.0, false)
                    .unwrap()
                    .out_edge_cursor()
                    .unwrap()
                    .into_edge_fields()
                    .map(|fd| {
                        assert_eq!(fd.len(), 1);
                        match fd.first().unwrap() {
                            FieldData::String(v) => v.clone(),
                            _ => panic!("field data should be string type"),
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            all_edge_fields,
            all_edges
                .iter()
                .map(|e| format!("edge_{}_{}", e.0, e.1))
                .collect()
        );
    } // end read-only transaction
}
