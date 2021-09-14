FROM rust:1.55.0

WORKDIR /usr/src/que-pasa
COPY . .

RUN cargo install --path .
CMD ["source env.example.sh; que-pasa"]