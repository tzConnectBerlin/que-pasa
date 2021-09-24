FROM rust:1.55.0

WORKDIR /usr/src/que-pasa
COPY . .

RUN cargo install --path .

ENV PATH "${PATH}:/usr/local/cargo/bin/"

CMD ["que-pasa"]