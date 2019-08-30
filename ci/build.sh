#!/usr/bin/env bash

set -euo pipefail

pushd /github/workspace
raco pkg install --auto --batch redis-lib/ redis-doc/ redis-test/
popd
