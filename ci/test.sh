#!/usr/bin/env bash

set -euo pipefail

pushd /github/workspace
env REDIS_HOST=redis raco test redis-lib/ redis-test/
popd
