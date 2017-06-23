CLI_EXECUTABLE=target/debug/tcc
DB_IMAGE_OK=.db-image.ok
DB_IMAGE_NAME=our/cassandra:latest
VIRTUAL_ENV=.python
CQLSH_EXECUTABLE=$(VIRTUAL_ENV)/bin/cqlsh

DB_PORT=9042
MAKESHELL=$(shell /usr/bin/env bash)

help:
	$(info Available Targets)
	$(info ---------------------------------------------------------------------------------------------------------------)
	$(info toc              | generate table of contents for README.md via doctoc)
	$(info unit-tests       | Run tests that don`t need a cassandra node running)
	$(info integration-tests| Run tests that use a cassandra node)
	$(info -- DEBUGGING --------------------------------------------------------------------------------------------------)
	$(info cli-execute      | Run the cli with certain arguments to help trying things - needs <some>-docker-db to be run before)
	$(info secrets          | generate all secrets with default passwords)
	$(info tls-tests        | Run cli against a TLS instance - needs tls-docker-db)
	$(info plain-docker-db  | Bring up a backgrounded cassandra database for local usage on 9042, optional TLS)
	$(info auth-docker-db   | Bring up a backgrounded cassandra database for local usage on 9042, optional TLS, requiring authentication)
	$(info cert-docker-db   | Bring up a backgrounded cassandra database for local usage on 9042, requiring the client to show a certificate)
	$(info attach-docker-db | run cassandra in foreground run with type=(tls|auth|plain))
	$(info cqlsh-execute    | run a cqlsh process with certain arguments against the local database)
	$(info fuzz             | try to run cargo-fuzz on the decoder - doesnt work right now)

toc:
	doctoc --github --title "A Cassandra Native Protocol 3 implementation using Tokio for IO." README.md
	
unit-tests: cli-tests
	cargo build                # Build with default features
	cargo build --all-features
	cargo doc --all-features
	cargo test --all-features

cli-tests: $(CLI_EXECUTABLE)
	bin/cli-tests.sh $(CLI_EXECUTABLE)

$(CLI_EXECUTABLE): $(shell find cli -name "*.rs")
	cd cli && cargo build --all-features

integration-tests: $(CLI_EXECUTABLE) $(DB_IMAGE_OK)
	bin/integration-test.sh $(CLI_EXECUTABLE) $(DB_IMAGE_NAME)
	
plain-docker-db: $(DB_IMAGE_OK)
	/usr/bin/env bash -c 'source lib/utilities.sh && start-dependencies-plain $(DB_IMAGE_NAME)'

auth-docker-db: $(DB_IMAGE_OK)
	/usr/bin/env bash -c 'source lib/utilities.sh && start-dependencies-auth $(DB_IMAGE_NAME)'

cert-docker-db: $(DB_IMAGE_OK)
	/usr/bin/env bash -c 'source lib/utilities.sh && start-dependencies-cert $(DB_IMAGE_NAME)'

type ?= plain
attach-docker-db:
	DEBUG_RUN_IMAGE=true $(MAKE) $(type)-docker-db

$(VIRTUAL_ENV):
	virtualenv -p python2.7 $@

$(CQLSH_EXECUTABLE): $(VIRTUAL_ENV)
	$(VIRTUAL_ENV)/bin/pip install cqlsh
	sed -i '' 's/DEFAULT_PROTOCOL_VERSION = 4/DEFAULT_PROTOCOL_VERSION = 3/g' $@

cqlsh-execute: $(CQLSH_EXECUTABLE)
	source $(VIRTUAL_ENV)/bin/activate && $(CQLSH_EXECUTABLE) localhost --cqlversion=3.2.1 -e "select * from system.batchlog"

cli-execute:
	cd cli && cargo run --all-features -- -h localhost query -o yaml -e cql-query

secrets:
	$(MAKE) -C etc/docker-cassandra $@

$(DB_IMAGE_OK): $(shell find etc/docker-cassandra -type f) bin/build-image.sh
	bin/build-image.sh etc/docker-cassandra $(DB_IMAGE_NAME)
	@touch $(DB_IMAGE_OK)

always-update:
	
.cargo-fuzz:
	git clone https://github.com/byron/cargo-fuzz $@
	$(MAKE) -C $@/etc/docker build
	
fuzz: always-update .cargo-fuzz
	docker run -v $$PWD:/source -w /source cargo-fuzz cargo fuzz run decoder
