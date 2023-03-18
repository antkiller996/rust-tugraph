`plugin-util` is a helper crate which make any rust function with following signature
```rust
fn (_: &mut Graph, _: &str) -> Result<String>
```
to be a plugin entry point.

# Example
```rust
use tugraph::{db::Graph, Result, txn::TxnRead};
use plugin_util::tugraph_plugin;

#[tugraph_plugin]
fn user_process_func(graph: &mut Graph, request: &str) -> Result<String> {
    // user process code
    Ok("Process Result".to_string())
}
```

It exports a attribute proc-macro `#[tugraph_plugin]` which can decorate a rust function and make the function expanded as:

```rust
use plugin_util::lgraph_api_graph_db_t;
use plugin_util::CxxString;
use plugin_util::Graph as TuGraph;

#[no_mangle]
pub unsafe extern "C" fn Process(
    graph_db: *mut lgraph_api_graph_db_t,
    request: *const CxxString,
    response: *mut CxxString) -> bool {
    let mut graph = TuGraph::from_ptr(graph_db);
    let request = if request.is_null() {
        ""
    } else {
        (*request).to_str().unwrap()
    };
    // user defined process function are nested here
    fn user_process_func(graph: &mut Graph, request: &str) -> Result<String> {
        // ...
    }
    let result = user_process_func(&mut graph, request);
    ::std::mem::forget(graph);
    
    match result {
        Ok(val) => {
            if !response.is_null() {
                let mut reponse = ::std::pin::Pin::new_unchecked(&mut *response);
                reponse.as_mut().clear();
                reponse.as_mut().push_str(val.as_str())
            }
            true
        }
        Err(e) => {
            eprintln!("run rust plugin failed: {:?}", e);
            false
        }
    }
}
```

