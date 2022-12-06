FROM rust:1.64 AS builder

WORKDIR /usr/src/que-pasa
COPY src src/
COPY sql sql/
COPY *.yaml ./
COPY *.sh ./
COPY Cargo.toml .
COPY askama.toml .

RUN cargo build --release --features health_check --verbose

# Using a slim debian as runtime image, rather than eg alpine.
# Reason: alpine requires static linking, which has some cons in rust
FROM debian:bullseye-slim

WORKDIR /que-pasa
COPY --from=builder /usr/src/que-pasa/target/release/que-pasa ./

RUN apt update
RUN apt -y install libssl1.1 libcurl4 dumb-init

ENTRYPOINT ["/usr/bin/dumb-init", "/que-pasa/que-pasa"]
