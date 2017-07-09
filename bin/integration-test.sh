#!/usr/bin/env bash

cli=${1:?Please provide the commandline interface executable as first argument}
image=${2:?Please provide the image name for the cassandra database}

# shellcheck disable=SC1091
# shellcheck source=../lib/utilities.sh
source "$(dirname "$0")/../lib/utilities.sh"

set -eu
port=$CASSANDRA_PORT
host=$CASSANDRA_HOST_NAME
ip=$CASSANDRA_HOST_IP
set +u

ca_file_args=( --ca-file ./etc/docker-cassandra/secrets/keystore.cer.pem )
con_ip_args=( -h $ip --port $port )
con_host_args=( -h $host --port $port )

trap stop-dependencies HUP INT TRAP TERM

start-cassandra-plain "$image"

#########################################################################
echo ">>>>>>>>>>>>>>>>>>>> Executing queries           <<<<<<<<<<<<<"
#########################################################################
set -x
$cli "${con_host_args[@]}" query -e "invalid syntax" && {
  echo "Should have failed due to invalid syntax"
  exit 20
}

$cli "${con_host_args[@]}" query -e "select * from system.batchlog" || {
  echo "it should have successfully executed a valid query"
  exit 21
}

#########################################################################
echo ">>>>>>>>>>>>>>>>>>>> TEST CONNECTION: PLAIN           <<<<<<<<<<<<<"
#########################################################################

set -x
$cli "${con_ip_args[@]}" test-connection
$cli "${con_host_args[@]}" test-connection
$cli --desired-cql-version 3.0.0 "${con_host_args[@]}" test-connection
$cli --desired-cql-version 2.0.0 "${con_host_args[@]}" test-connection \
  && { echo "server cannot handle versions that are too low"; exit 6; }

$cli "${con_ip_args[@]}" --tls "${ca_file_args[@]}"  test-connection \
  && { echo "should not connect if ip is set when using tls - verification must fail"; exit 1; }
$cli "${con_host_args[@]}" --tls "${ca_file_args[@]}"  test-connection 
$cli "${con_host_args[@]}" "${ca_file_args[@]}"  test-connection \
  || { echo "should imply tls if CA-file is specified"; exit 2; }
$cli "${con_host_args[@]}" --tls test-connection \
  && { echo "should fail TLS hostname verification on self-signed cert by default"; exit 3; }
set +x

#########################################################################
echo ">>>>>>>>>>>>>>>>>>>> TEST CONNECTION: WITH-AUTHENTICATION <<<<<<<<"
#########################################################################
start-cassandra-auth "$image"
# YES - there is something async going on, so we have to give it even more time until 
# it can accept properly authenticated connections
sleep 1

auth_args=( -u cassandra -p cassandra ) 

set -x
$cli "${auth_args[@]}" "${con_ip_args[@]}" test-connection
$cli "${auth_args[@]}" "${con_host_args[@]}" "${ca_file_args[@]}" test-connection
set +x


#########################################################################
echo ">>>>>>>>>>>>>>>>>>>> TEST CONNECTION: WITH-CERTIFICATE <<<<<<<<"
#########################################################################
start-cassandra-cert "$image"


cert_args=( --cert ./etc/docker-cassandra/secrets/keystore.p12:cassandra )

set -x
$cli "${con_host_args[@]}" --cert-type pkcs12 "${cert_args[@]}" "${ca_file_args[@]}" test-connection
$cli "${con_host_args[@]}" "${cert_args[@]}" "${ca_file_args[@]}" test-connection \
  || { echo "cert-type PK12 is defaulting to the one type we currently know"; exit 4; }
$cli "${con_host_args[@]}" "${ca_file_args[@]}" test-connection \
  && { echo "it doesnt work with without a certificate as server requires client cert"; exit 5; }

set +x
