#!/bin/bash
set -e

# Install registry CA certificate if mounted
if [ -f "/tmp/registry-ca/tls.crt" ]; then
  echo "Installing registry CA certificate..."
  cp /tmp/registry-ca/tls.crt /usr/local/share/ca-certificates/clotho-registry.crt
  update-ca-certificates
  echo "CA certificate installed successfully"
fi

REPO_URL=$1
REF=$2
IMAGE_TAG=$3
PROJECT_PATH=${4:-.}
RUNTIME=${5:-wasm}

echo "Starting Build for $REPO_URL @ $REF (path: $PROJECT_PATH, runtime: $RUNTIME)"

# A. Clone (with token auth if GIT_TOKEN is set)
if [ -n "$GIT_TOKEN" ]; then
  AUTH_URL=$(echo $REPO_URL | sed "s|https://|https://x-access-token:$GIT_TOKEN@|")
  git clone $AUTH_URL /app/source
  git config --global credential.helper store
  echo "https://x-access-token:$GIT_TOKEN@github.com" > ~/.git-credentials
  export CARGO_NET_GIT_FETCH_WITH_CLI=true
else
  git clone $REPO_URL /app/source
fi
cd /app/source
git checkout $REF

# Navigate to project path if specified
cd $PROJECT_PATH

# B. Build based on runtime
if [ "$RUNTIME" = "native" ]; then
  # -------------------------------------------------------
  # Native Build: compile to Linux binary, package with crane
  # -------------------------------------------------------

  # Clone the Clotho SDK repo if CLOTHO_SDK_REPO is set.
  # Native pipelines use clotho-sdk as a local path dependency.
  # The path dep (../../../clotho/clotho-sdk) resolves to /app/clotho/clotho-sdk.
  if [ -n "$CLOTHO_SDK_REPO" ]; then
    echo "Cloning Clotho SDK..."
    SDK_REF=${CLOTHO_SDK_REF:-main}
    if [ -n "$GIT_TOKEN" ]; then
      SDK_AUTH_URL=$(echo $CLOTHO_SDK_REPO | sed "s|https://|https://x-access-token:$GIT_TOKEN@|")
      git clone --depth 1 -b "$SDK_REF" "$SDK_AUTH_URL" /app/clotho
    else
      git clone --depth 1 -b "$SDK_REF" "$CLOTHO_SDK_REPO" /app/clotho
    fi
    echo "Clotho SDK cloned to /app/clotho"
  fi

  echo "Compiling Rust (native)..."
  # Override release profile to reduce peak memory usage in constrained pods.
  # LTO and codegen-units=1 cause massive memory spikes during linking.
  export CARGO_PROFILE_RELEASE_LTO=off
  export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
  CARGO_BUILD_JOBS=1 cargo build --release

  # Collect all compiled binaries from target/release
  # Rust release binaries have no file extension; skip .d, .rlib, etc.
  mkdir -p /tmp/image-root/app
  for bin in target/release/*; do
    basename_bin=$(basename "$bin")
    # Skip files with extensions (e.g. .d, .rlib, .rmeta, .so) and hidden/build dirs
    case "$basename_bin" in
      *.* | build | deps | examples | incremental | .fingerprint) continue ;;
    esac
    if [ -f "$bin" ] && [ -x "$bin" ]; then
      cp "$bin" /tmp/image-root/app/
      echo "  Found binary: $basename_bin"
    fi
  done

  # Check we found at least one binary
  BINARY_COUNT=$(ls /tmp/image-root/app/ | wc -l)
  if [ "$BINARY_COUNT" -eq 0 ]; then
    echo "ERROR: No ELF binaries found in target/release/"
    exit 1
  fi
  echo "Collected $BINARY_COUNT binaries"

  # Copy shared library dependencies into the image
  # The binaries are dynamically linked and need libssl, libcrypto, etc.
  mkdir -p /tmp/image-root/lib/x86_64-linux-gnu /tmp/image-root/lib64
  for bin in /tmp/image-root/app/*; do
    ldd "$bin" 2>/dev/null | grep "=> /" | awk '{print $3}' | while read lib; do
      cp -n "$lib" /tmp/image-root/lib/x86_64-linux-gnu/ 2>/dev/null || true
    done
  done
  # Copy the dynamic linker
  cp /lib64/ld-linux-x86-64.so.2 /tmp/image-root/lib64/ 2>/dev/null || true
  # Copy CA certificates for TLS (needed for WSS, HTTPS, MongoDB TLS, etc.)
  mkdir -p /tmp/image-root/etc/ssl/certs
  cp /etc/ssl/certs/ca-certificates.crt /tmp/image-root/etc/ssl/certs/ 2>/dev/null || true
  echo "  Copied shared libraries and CA certificates"

  # Create OCI layer tarball
  tar -C /tmp/image-root -cf /tmp/layer.tar app/ lib/ lib64/ etc/

  # Build and push OCI image using crane
  # Base image: debian:bookworm-slim (matches build environment)
  echo "Packaging and pushing to $IMAGE_TAG..."
  crane append \
    --base debian:bookworm-slim \
    -f /tmp/layer.tar \
    -t "$IMAGE_TAG"

  echo "Native build complete!"

else
  # -------------------------------------------------------
  # WASM Build: compile to wasm32-wasip1, push via Spin CLI
  # -------------------------------------------------------
  echo "Compiling Rust to Wasm..."
  CARGO_BUILD_JOBS=1 cargo build --target wasm32-wasip1 --release

  echo "Packaging and Pushing to $IMAGE_TAG..."
  spin registry push $IMAGE_TAG

  echo "WASM build complete!"
fi
