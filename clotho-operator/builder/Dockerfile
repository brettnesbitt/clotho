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

# 3.5. Install CA certificate for internal registry (will be mounted at runtime)
RUN mkdir -p /etc/docker/certs.d/clotho-registry.clotho-system.svc.cluster.local:5000

# 4. Create the Build Script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Install registry CA certificate if mounted\n\
if [ -f "/tmp/registry-ca/tls.crt" ]; then\n\
  echo "Installing registry CA certificate..."\n\
  cp /tmp/registry-ca/tls.crt /usr/local/share/ca-certificates/clotho-registry.crt\n\
  update-ca-certificates\n\
  echo "CA certificate installed successfully"\n\
fi\n\
\n\
REPO_URL=$1\n\
REF=$2\n\
IMAGE_TAG=$3\n\
PROJECT_PATH=${4:-.}\n\
\n\
echo "🚀 Starting Build for $REPO_URL @ $REF (path: $PROJECT_PATH)"\n\
\n\
# A. Clone (with token auth if GIT_TOKEN is set)\n\
if [ -n "$GIT_TOKEN" ]; then\n\
  # Insert token into URL for private repos (use x-access-token for GitHub PATs)\n\
  AUTH_URL=$(echo $REPO_URL | sed "s|https://|https://x-access-token:$GIT_TOKEN@|")\n\
  git clone $AUTH_URL /app/source\n\
else\n\
  git clone $REPO_URL /app/source\n\
fi\n\
cd /app/source\n\
git checkout $REF\n\
\n\
# Navigate to project path if specified\n\
cd $PROJECT_PATH\n\
\n\
# B. Build (Release Mode)\n\
echo "🔨 Compiling Rust to Wasm..."\n\
cargo build --target wasm32-wasip1 --release\n\
\n\
# C. Package & Push to Internal Registry (TLS enabled, no auth needed)\n\
echo "📦 Packaging and Pushing to $IMAGE_TAG..."\n\
spin registry push $IMAGE_TAG\n\
\n\
echo "✅ Build Complete!"\n\
' > /usr/local/bin/builder.sh && chmod +x /usr/local/bin/builder.sh

WORKDIR /app
ENTRYPOINT ["/usr/local/bin/builder.sh"]