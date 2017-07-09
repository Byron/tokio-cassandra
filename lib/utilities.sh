#!/bin/bash
CONTAINER_NAME=db
CASSANDRA_PORT=9042
CASSANDRA_HOST_IP=127.0.0.1
CASSANDRA_HOST_NAME=localhost

read -r -d '' ENV_FILE <<EOF
CASSANDRA_ENABLE_SSL=true
# turn this on to require SSL in the client. Use the client.cer as certificate,
# as it is trusted already.
# effectively allow client authentication - optional: true allows both connection types,
# which is useful for our case as we have to restart the server less often
CASSANDRA_REQUIRE_CLIENT_AUTH=true
CASSANDRA_REQUIRE_CLIENT_CERTIFICATE=false
	CASSANDRA_ENABLE_SSL_DEBUG=true
	CASSANDRA_KEYSTORE_PASSWORD=cassandra
	CASSANDRA_TRUSTSTORE_PASSWORD=cassandra
	CASSANDRA_SSL_PROTOCOL=TLS
	CASSANDRA_SSL_ALGORITHM=SunX509
    CASSANDRA_KEYSTORE_PATH=/config/keystore
	CASSANDRA_KEYSTORE_PASSWORD=cassandra
	CASSANDRA_TRUSTSTORE_PATH=/config/truststore
	CASSANDRA_TRUSTSTORE_PASSWORD=cassandra
CASSANDRA_AUTHENTICATOR=AllowAllAuthenticator
# CASSANDRA_AUTHENTICATOR=PasswordAuthenticator
	CASSANDRA_ADMIN_USER=cassandra
	CASSANDRA_ADMIN_PASSEORD=cassandra
EOF

start-image() {
	local IMAGE_NAME=${1:?Need cassandra image name}
	local TESTER=${2:?Need command line to execute to see if cassandra is up}
	local ADD_ARGS=${3:-} # optional additional arguments
	echo starting dependencies
	local debug_mode=${DEBUG_RUN_IMAGE:-false}
	local daemonize="-d"
	if [ "$debug_mode" = true ]; then
		daemonize=''
	fi
	docker rm --force $CONTAINER_NAME || true;
	docker run --name "$CONTAINER_NAME" --env-file <(echo "$ENV_FILE") $ADD_ARGS $daemonize -p "$CASSANDRA_HOST_IP":"$CASSANDRA_PORT":9042 --expose 9042 $IMAGE_NAME 1>&2 || exit $?
	
	if [ "$debug_mode" = false ]; then
		local retries_left=15
		while ! $TESTER "$CASSANDRA_HOST_NAME" "$CASSANDRA_PORT" && [ $retries_left != 0 ]; do
			echo "Waiting for cassandra on $CASSANDRA_HOST_NAME:$CASSANDRA_PORT, retries-left=$retries_left" 1>&2
			sleep 2
			((retries_left-=1))
		done
		if [ $retries_left = 0 ]; then
			echo "Could not connect to cassandra - may be a problem with '$TESTER', or cassandra itself" 1>&2
			return 3
		fi
		echo "Cassandra up on $CASSANDRA_HOST_NAME:$CASSANDRA_PORT" 1>&2
	fi
}

function test-tls () {
	if [ "`uname -s`" = Linux ]; then
		curl -m 1 --key ./etc/docker-cassandra/secrets/keystore.key.pem \
							--cert ./etc/docker-cassandra/secrets/keystore.cer.pem -k https://$1:$2
	else
		curl -m 1 -E ./etc/docker-cassandra/secrets/keystore.p12:cassandra -k https://$1:$2
	fi
	[ $? = 28 ]
}

function test-simple () {
	curl -m 1 http://$1:$2
   [ $? = 28 ]
}

function start-cassandra-plain () {
	start-image "$1" test-simple
}

function start-cassandra-auth () {
	start-image "$1" test-simple "-e CASSANDRA_AUTHENTICATOR=PasswordAuthenticator"
}

function start-cassandra-cert () {
	start-image "$1" test-tls "-e CASSANDRA_REQUIRE_CLIENT_CERTIFICATE=true"
}

stop-dependencies() {
	echo stopping dependencies ...
	docker rm --force $CONTAINER_NAME >/dev/null
}
