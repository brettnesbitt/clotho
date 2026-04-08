FROM rust:1.94-slim-bookworm

# 1. Install System Dependencies
RUN apt-get update && apt-get install -y \
    git \
    pkg-config \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Spin CLI (Latest)
RUN curl -fsSL https://developer.fermyon.com/downloads/install.sh | bash && \
    mv spin /usr/local/bin/

# 3. Add Wasm Target
RUN rustup target add wasm32-wasip1

# 3.1. Install WASI SDK (needed by ring crate to compile C code for wasm32-wasip1)
ENV WASI_VERSION=25
ENV WASI_SDK_PATH=/opt/wasi-sdk
RUN curl -fsSL https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_VERSION}/wasi-sdk-${WASI_VERSION}.0-x86_64-linux.tar.gz \
    | tar xz -C /opt && mv /opt/wasi-sdk-${WASI_VERSION}.0-x86_64-linux ${WASI_SDK_PATH}
ENV CC_wasm32_wasip1="${WASI_SDK_PATH}/bin/clang --sysroot=${WASI_SDK_PATH}/share/wasi-sysroot"

# 3.2. Install crane for native OCI image creation (no Docker daemon needed)
RUN curl -sL https://github.com/google/go-containerregistry/releases/download/v0.20.2/go-containerregistry_Linux_x86_64.tar.gz \
    | tar xz -C /usr/local/bin/ crane

# 3.5. Install CA certificate for internal registry (will be mounted at runtime)
RUN mkdir -p /etc/docker/certs.d/clotho-registry.clotho-system.svc.cluster.local:5000

# 4. Create the Build Script
COPY builder.sh /usr/local/bin/builder.sh
RUN chmod +x /usr/local/bin/builder.sh

WORKDIR /app
ENTRYPOINT ["/usr/local/bin/builder.sh"]