# ------------------------------------------------------------------------------
# Cargo Build Stage
# ------------------------------------------------------------------------------

# 1: Build the exe
FROM rust:latest as builder
WORKDIR /usr/src

# 1a: Prepare for static linking
RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y musl-tools && \
    rustup target add x86_64-unknown-linux-musl

WORKDIR /home/rust/
COPY Cargo.toml .
COPY Cargo.lock .

RUN mkdir src
RUN echo "fn main() {}" > src/main.rs
RUN cargo build --release --target=x86_64-unknown-linux-musl

# 1c: Build the exe using the actual source code
ADD --chown=rust:rust . ./

RUN touch src/main.rs
RUN cargo build --release --target=x86_64-unknown-linux-musl

RUN strip target/x86_64-unknown-linux-musl/release/rust-s3-scylladb-svc

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

# Start building the final image
FROM apsops/scratch-n-cacerts
WORKDIR /home/rust/
COPY --from=builder /home/rust/target/x86_64-unknown-linux-musl/release/rust-s3-scylladb-svc .
COPY schema/ddl.sql .
ENTRYPOINT ["./rust-s3-scylladb-svc"]