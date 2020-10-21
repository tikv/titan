#!/bin/bash

set -ev
cmake . -L -DTRAVIS=ON ${COMPRESSION_OPT} ${BUILD_OPT} ${SANITIZER_OPT} ${TOOLS_OPT} ${COVERAGE_OPT}
make -j4
ctest -R titan

