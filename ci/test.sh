#!/bin/bash

set -e
set -x

if [ "$RUN_TYPE" = "test" ]; then
   cd $(dirname "$0")/../build/tests
   exec ctest -j3 --output-on-failure && ../libraries/vendor/mira/test/mira_test

if ! [[ -z $BUILD_DOCKER ]]; then
   TAG="$TRAVIS_BRANCH"
   if [ "$TAG" = "master" ]; then
      TAG="latest"
   fi

   export MEMPOOL_TAG=$TAG

   git clone https://github.com/koinos/koinos-integration-tests.git

   cd koinos-integration-tests
   go get ./...
   cd tests
   ./run.sh
fi
