#! /bin/sh


find . \
    -name '.git' -prune -o \
    -name 'dist' -prune -o \
    -name 'build' -prune -o \
    -name '*.egg-info' -prune -o \
    -name '__pycache__' -prune -o \
    -name '.pytype' -prune -o \
    -name '.mypy_cache' -prune -o \
    -name '.tox' -prune -o \
    -name 'data' -prune -o \
    -name '*.log' -prune -o \
    -name '*.pid' -prune -o \
    -name 'tmp' -prune -o \
    -name '*.swp' -prune -o \
    -print

