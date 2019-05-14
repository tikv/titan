#!/bin/bash

git diff `git merge-base master HEAD` | clang-format-diff -style=google -p1 -i
