#!/bin/bash

set -ev
cmake . -L -DROCKSDB_DIR=./rocksdb -DTRAVIS=ON ${BUILD_OPT} ${SANITIZER_OPT} ${TOOLS_OPT} ${COVERAGE_OPT}
make -j4
ctest -R titan

