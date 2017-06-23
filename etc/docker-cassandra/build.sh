#!/bin/bash

set -e

CASSANDRA_VERSION="2.2.5"
IMAGE_TAG="pitrho/cassandra"

# Custom die function.
#
die() { echo -e "\nRUN ERROR: " "$@" "\n" >&2 ; exit 1; }

# Parse the command line flags.
#
while getopts "v:t:" opt; do
  case $opt in
    t)
      IMAGE_TAG=${OPTARG}
      ;;

    v)
      CASSANDRA_VERSION=${OPTARG}
      ;;

    \?)
      die "Invalid option: -$OPTARG"
      ;;
  esac
done

# Crete the build directory
rm -rf build
mkdir build

cp -R secrets build/
cp run.sh build/
cp cassandra.yaml build/
cp cqlshrc build/

# Copy docker file, and override the CASSANDRA_VERSION string
sed 's/%%CASSANDRA_VERSION%%/'"$CASSANDRA_VERSION"'/g' Dockerfile.tmpl > build/Dockerfile

docker build -t="${IMAGE_TAG}" build/

rm -rf build
