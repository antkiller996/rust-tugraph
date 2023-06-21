# Copyright 2023 antkiller
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


SCRIPT_DIR=$(cd $(dirname $0); pwd)

PROJECT_DIR=$SCRIPT_DIR/..

set -e

[ -d /tmp/rust_tugraph ] && rm -rf /tmp/rust_tugraph

echo "clean target.."
cargo clean

echo "cargo test --lib --verbose && cargo test --tests --verbose..."
cargo test --lib --verbose && cargo test --tests --verbose

# doctest not load same shared libraries as cargo test --lib
# see issue: https://github.com/rust-lang/cargo/issues/8531
# find /path/to/out/liblgraph.so by find command and set LD_LIBRARY_PATH=/path/to/out
echo "cargo test --doc --verbose"
LD_LIBRARY_PATH=`find $PROJECT_DIR/target -name "liblgraph.so" -print | xargs -n 1 dirname` cargo test --doc --verbose

