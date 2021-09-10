FROM rust:1.31

WORKDIR /usr/src/que-pasa
COPY . .

RUN cargo install --path .
CMD ["que-pasa"]