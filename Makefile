
# export NODE_URL=https://testnet-tezos.giganode.io
# export CONTRACT_ID=KT18sHKbZtXhXtnf6ZrHEW9VgEe2eCvRr2CS
export NODE_URL=https://mainnet-tezos.giganode.io
export CONTRACT_ID=KT1QxLqukyfohPV5kPkw97Rs6cw1DDDvYgbB
export DATABASE_URL=host=0.0.0.0 dbname=tezos user=quepasa password=quepasa port=5432
NETWORK="mainnet"
# BLOCKS=245893,245894

gen-sql:
ifeq ($(strip $(CONTRACT_ID)),"")
	$(error variable CONTRACT_ID not set)
else
	cargo +nightly run -- generate-sql > contract.sql/init.sql
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
	cargo +nightly run -- -l 248654
endif

fill:
ifeq ($(strip $(CONTRACT_ID)),"")
	$(error variable CONTRACT_ID not set)
else
	$(eval BLOCKS := $(shell python3 ./script/get-levels.py $(NETWORK) $(CONTRACT_ID)))
	cargo +nightly run -- -l $(BLOCKS)
endif

db:
	make gen-sql
	make start-db
