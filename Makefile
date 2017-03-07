CLI_EXECUTABLE=target/debug/tcc
DB_IMAGE_OK=.db-image.ok
DB_IMAGE_NAME=our/cassandra:latest

DB_PORT=9042
MAKESHELL=$(shell /usr/bin/env bash)

help:
	$(info Available Targets)
	$(info ---------------------------------------------------------------------------------------------------------------)
	$(info toc              | generate table of contents for README.md via doctoc)
	$(info unit-tests       | Run tests that don`t need a cassandra node running)
	$(info integration-tests| Run tests that use a cassandra node)
	$(info -- DEBUGGING --------------------------------------------------------------------------------------------------)
	$(info cli-tests        | Run the cli with certain arguments to help debugging - needs <some>-docker-db)
	$(info secrets          | generate all secrets with default passwords)
	$(info tls-tests        | Run cli against a TLS instance - needs tls-docker-db)
	$(info plain-docker-db  | Bring up a backgrounded cassandra database for local usage on 9042, optional TLS)
	$(info auth-docker-db   | Bring up a backgrounded cassandra database for local usage on 9042, optional TLS, requiring authentication)
	$(info cert-docker-db   | Bring up a backgrounded cassandra database for local usage on 9042, requiring the client to show a certificate)
	$(info attach-docker-db | run cassandra in foreground run with type=(tls|auth|plain))
	$(info fuzz             | try to run cargo-fuzz on the decoder - doesn't work right now)

toc:
	doctoc --github --title "A Cassandra Native Protocol 3 implementation using Tokio for IO." README.md
	
unit-tests:
	cargo build # Build with default features
	cargo doc --all-features
	cargo test --all-features

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

cli-tests:
	cd cli && cargo run --all-features -- --cert-type PK12 --cert ../etc/docker-cassandra/secrets/keystore.p12:cassandra --ca-file ../etc/docker-cassandra/secrets/keystore.cer.pem -h localhost test-connection

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
	docker run -v $$PWD:/source cargo-fuzz cargo fuzz run decoder
