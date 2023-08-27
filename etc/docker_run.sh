#! /bin/bash

# python3 setup.py build -t ../tmp -b ../tmp


set -e
set -x

copy_repo() {
    mkdir -p "$2"
    cd "$1"
    ./etc/showfiles.sh | cpio -p --no-preserve-owner -d -m "$2"
    chmod -R u+w "$2"
    cd /code
}

pg_start() {
    cd /code
    pg_ctl -D data -l log/pg.log start || { cat log/pg.log ; exit 1; }
}

#cd /repo
#pip3 -q --disable-pip-version-check --no-cache-dir install --user -r /repo/etc/requirements.txt

chmod -R u+w /code/src || true
rm -rf /code/src /code/tests
copy_repo /repo /code/src

cd /code/src
python3 setup.py -q sdist
pip3 install --disable-pip-version-check --no-cache-dir --no-deps --user dist/*
cp -rp tests /code

pg_start

cd /code/tests

exec "$@"
