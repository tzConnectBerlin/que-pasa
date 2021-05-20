
CONTRACT=""
NETWORK="edo2net"
BLOCKS=""
NODE_URL = "http://edo2full.newby.org:8732"

gen-sql:
ifeq ($(strip $(CONTRACT)),"")
	$(error variable CONTRACT not set)
else
	cargo run -- -c $(CONTRACT) generate-sql > contract.sql/init.sql
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
ifeq ($(strip $(CONTRACT)),"")
	$(error variable CONTRACT not set)
else
							cargo run -- -c $(CONTRACT)
endif

fill:
ifeq ($(strip $(CONTRACT)),"")
	$(error variable CONTRACT not set)
else
		$(eval BLOCKS := $(shell python ./script/get-levels.py $(NETWORK) $(CONTRACT)))
		cargo run -- -c $(CONTRACT) -l $(BLOCKS)
endif

db:
	make gen-sql
	make start-db
