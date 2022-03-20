#!/bin/bash

set -ev
git fetch --depth=1 origin master:master;
git diff $(git merge-base master HEAD) HEAD > diff;
cat diff | clang-format-diff-10 -style=google -p1 > formatted;
if [ -s formatted ]; then
  cat formatted;
  echo "Run scripts/format-diff.sh to format your code.";
  exit 1;
fi;
