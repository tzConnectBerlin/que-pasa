export DATABASE_URL=host=0.0.0.0 dbname=tezos user=quepasa password=quepasa port=5432
export CONTRACT_SETTINGS=settings.yaml
# BLOCKS=245893,245894


# GRANADA testnet:
export NODE_URL=https://testnet-tezos.giganode.io
NETWORK="granadanet"

# HEN on GRANADA:
#export NODE_URL=https://mainnet-tezos.giganode.io
#NETWORK="mainnet"


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
	RUST_BACKTRACE=1 cargo run
endif

fill:
ifeq ($(strip $(CONTRACT_ID)),"")
	$(error variable CONTRACT_ID not set)
else
	RUST_BACKTRACE=1 cargo run -- --init --bcd-url https://api.better-call.dev/v1 --network $(NETWORK)
endif

db:
	make start-db
