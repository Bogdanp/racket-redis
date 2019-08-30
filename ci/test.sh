#!/usr/bin/env bash

set -euo pipefail

pushd /github/workspace
env REDIS_HOST=redis raco test --drdr redis-lib/ redis-test/
popd
