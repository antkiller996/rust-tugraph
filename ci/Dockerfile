FROM tugraph/tugraph-compile-ubuntu18.04:latest

ARG RUST_VERSION=1.68.0

# upgrade git and install libclang-*.so
RUN apt update -y \
    && apt install -y software-properties-common \
    && add-apt-repository ppa:git-core/ppa \
    && apt update -y \
    && apt remove -y git && apt install -y git \
    && apt install -y libclang-10-dev \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH="/usr/local/cargo/bin:$PATH"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --profile default --default-toolchain ${RUST_VERSION} \
    && chmod -R a+w ${RUSTUP_HOME} ${CARGO_HOME};