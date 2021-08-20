export DATABASE_URL=host=0.0.0.0 dbname=tezos user=quepasa password=quepasa port=5432
# BLOCKS=245893,245894


# PMM on GRANADA testnet:

export NODE_URL=https://testnet-tezos.giganode.io
export CONTRACT_ID=KT18sHKbZtXhXtnf6ZrHEW9VgEe2eCvRr2CS
NETWORK="granadanet"


# HEN on GRANADA:

# export NODE_URL=https://mainnet-tezos.giganode.io
# export CONTRACT_ID=KT1QxLqukyfohPV5kPkw97Rs6cw1DDDvYgbB
# NETWORK="mainnet"


gen-sql:
ifeq ($(strip $(CONTRACT_ID)),"")
	$(error variable CONTRACT_ID not set)
else
	RUST_BACKTRACE=1 cargo +nightly run -- generate-sql > contract.sql/init.sql
endif

start-db:
	docker-compose up -d

down-db:
	docker-compose down

destroy-db:
	docker-compose down -v

start-graphql:
	cd graphql && npm install && npm start

start-indexer:
ifeq ($(strip $(CONTRACT_ID)),"")
	$(error variable CONTRACT_ID not set)
else
	RUST_BACKTRACE=1 cargo +nightly run -- -l 248654
endif

fill:
ifeq ($(strip $(CONTRACT_ID)),"")
	$(error variable CONTRACT_ID not set)
else
	RUST_BACKTRACE=1 cargo +nightly run -- --init --bcd-url https://api.better-call.dev/v1 --network $(NETWORK)
endif

db:
	make gen-sql
	make start-db
