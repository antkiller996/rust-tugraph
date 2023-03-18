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

use chrono::NaiveDateTime;
use tugraph::{
    cursor::{EdgeCursor, EdgeCursorMut, VertexCursor, VertexCursorMut},
    db::{Graph, OpenOptions, MINIMUM_GRAPH_MAX_SIZE},
    field::{FieldData, FieldSpec, FieldType},
    txn::{RwTxn, TxnRead, TxnWrite},
    types::{DateTime, EdgeUid},
};

mod common;

#[test]
fn test_create_graph() {
    let tmpdir = tempfile::tempdir().unwrap();
    let galaxy = OpenOptions::new()
        .create(true)
        .open(tmpdir.into_path(), "admin", "73@TuGraph")
        .unwrap();
    let _ = galaxy
        .create_graph(
            "test_create_graph",
            "graph created in test_create_graph",
            MINIMUM_GRAPH_MAX_SIZE,
        )
        .unwrap();
}

#[test]
fn test_graph_add() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let default_graph = galaxy.open_graph("default", false).unwrap();
    add_person_post_comment_schema(&default_graph);

    let mut rw_txn = default_graph.create_rw_txn(false).unwrap();
    let alice = add_person(&mut rw_txn, "Alice".to_string(), 17, false).unwrap();
    let comment = add_comment(&mut rw_txn, "Alice is 17 years old".to_string()).unwrap();
    let post = add_post(
        &mut rw_txn,
        alice,
        chrono::offset::Utc::now().naive_utc(),
        comment,
    )
    .unwrap();
    rw_txn.commit().unwrap();

    let ro_txn = default_graph.create_ro_txn().unwrap();

    //check vids
    let vids: Vec<_> = ro_txn.vertex_cur().unwrap().into_vertex_ids().collect();
    assert_eq!(vids, vec![alice, comment]);

    // check edges
    let mut vcur = ro_txn.vertex_cur().unwrap();
    vcur.seek(alice, true).expect("seek to alice should be ok");
    let edges: Vec<_> = vcur
        .out_edge_cursor()
        .map(|oecur| oecur.into_edge_uids().collect())
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0], post);
}

#[test]
fn test_graph_mod() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let default_graph = galaxy.open_graph("default", false).unwrap();
    add_person_post_comment_schema(&default_graph);

    let mut rw_txn = default_graph.create_rw_txn(false).unwrap();
    let alice = add_person(&mut rw_txn, "Alice".to_string(), 17, false).unwrap();
    let comment = add_comment(&mut rw_txn, "Alice is 18 years old".to_string()).unwrap();
    let _ = add_post(
        &mut rw_txn,
        alice,
        chrono::offset::Utc::now().naive_utc(),
        comment,
    )
    .unwrap();
    let mut vcur_mut = rw_txn.vertex_cur_mut().unwrap();
    assert!(vcur_mut.is_valid());
    vcur_mut.set_field("age", &FieldData::Int8(20)).unwrap();
    assert!(vcur_mut.seek_to_next().unwrap().is_some());
    assert!(vcur_mut.is_valid());
    vcur_mut
        .set_field(
            "content",
            &FieldData::String("Alice is 20 years old".to_string()),
        )
        .unwrap();

    vcur_mut
        .seek(alice, true)
        .expect("seek to alice should be ok");
    let oecur_mut = vcur_mut.out_edge_cursor_mut().unwrap();
    assert!(oecur_mut.is_valid());
    let changed_datatime = chrono::offset::Utc::now().naive_utc().timestamp();
    oecur_mut
        .set_field(
            "datetime",
            &FieldData::DateTime(DateTime::from_timestamp_opt(changed_datatime).unwrap()),
        )
        .unwrap();
    rw_txn.commit().unwrap();

    let ro_txn = default_graph.create_ro_txn().unwrap();
    let mut vcur = ro_txn.vertex_cur().unwrap();
    assert!(vcur.is_valid());
    assert_eq!(vcur.id().unwrap(), alice);
    assert_eq!(vcur.field("age").unwrap(), FieldData::Int8(20));
    assert!(vcur.seek_to_next().unwrap().is_some());
    assert!(vcur.is_valid());
    assert_eq!(
        vcur.field("content").unwrap(),
        FieldData::String("Alice is 20 years old".to_string())
    );
    vcur.seek(alice, true).expect("seek to alice should be ok");
    let oecur = vcur.out_edge_cursor().unwrap();
    assert_eq!(
        oecur.field("datetime").unwrap(),
        FieldData::DateTime(DateTime::from_timestamp_opt(changed_datatime).unwrap())
    )
}

fn person_label() -> (&'static str, [FieldSpec; 3], &'static str) {
    (
        "Person",
        [
            FieldSpec {
                name: "name".into(),
                ty: FieldType::String,
                optional: false,
            },
            FieldSpec {
                name: "age".into(),
                ty: FieldType::Int8,
                optional: false,
            },
            FieldSpec {
                name: "is_male".into(),
                ty: FieldType::Bool,
                optional: false,
            },
        ],
        "name",
    )
}

fn add_person(
    rw_txn: &mut RwTxn,
    name: String,
    age: i8,
    is_male: bool,
) -> Result<i64, tugraph::Error> {
    rw_txn.add_vertex(
        "Person",
        &["name", "age", "is_male"],
        &[
            FieldData::String(name),
            FieldData::Int8(age),
            FieldData::Bool(is_male),
        ],
    )
}

fn comment_label() -> (&'static str, [FieldSpec; 1], &'static str) {
    (
        "Comment",
        [FieldSpec {
            name: "content".into(),
            ty: FieldType::String,
            optional: false,
        }],
        "content",
    )
}

fn add_comment(rw_txn: &mut RwTxn, content: String) -> Result<i64, tugraph::Error> {
    rw_txn.add_vertex("Comment", &["content"], &[FieldData::String(content)])
}

fn post_label() -> (
    &'static str,
    [FieldSpec; 1],
    &'static str,
    [(&'static str, &'static str); 1],
) {
    (
        "Post",
        [FieldSpec {
            name: "datetime".into(),
            ty: FieldType::DateTime,
            optional: false,
        }],
        "",
        [("Person", "Comment")],
    )
}

fn add_post(
    rw_txn: &mut RwTxn,
    person: i64,
    datetime: NaiveDateTime,
    comment: i64,
) -> Result<EdgeUid, tugraph::Error> {
    rw_txn.add_edge(
        person,
        comment,
        "Post",
        &["datetime"],
        &[FieldData::DateTime(DateTime::from_native(datetime))],
    )
}

fn add_person_post_comment_schema(graph: &Graph) {
    // add labels
    let person = person_label();
    let ret = graph
        .add_vertex_label(person.0, &person.1, person.2)
        .unwrap();
    assert!(ret);
    let comment = comment_label();
    let ret = graph
        .add_vertex_label(comment.0, &comment.1, comment.2)
        .unwrap();
    assert!(ret);
    let post = post_label();
    let ret = graph
        .add_edge_label(post.0, &post.1, post.2, post.3)
        .unwrap();
    assert!(ret);
}
