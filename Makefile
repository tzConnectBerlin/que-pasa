CONTRACT=""
NETWORK="edo2net"
BLOCKS=""

gen-sql:
				cargo run -- -c $(CONTRACT) generate-sql > contract.sql

start-db:
				docker-compose up -d

down-db:
				docker-compose down

destroy-db:
					docker-compose down -v

start-graphql:
							cd graphql && npm install && npm start

start-indexer:
							cargo run -- -c $(CONTRACT)

fill:
		$(eval BLOCKS := $(shell python ./script/get-levels.py $(NETWORK) $(CONTRACT)))
		cargo run -- -c $(CONTRACT) -l $(BLOCKS)

db:
	make gen-sql
	make start-db