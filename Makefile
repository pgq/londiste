
VERSION = $(shell python3 setup.py --version)
RXVERSION = $(shell python3 setup.py --version | sed 's/\./[.]/g')
TAG = v$(VERSION)
NEWS = NEWS.rst

all: lint test

test:
	tox -e py38

lint:
	tox -q -e lint

xlint:
	tox -q -e xlint

clean:
	rm -rf build *.egg-info */__pycache__ tests/*.pyc
	rm -rf .pybuild MANIFEST

xclean: clean
	rm -rf .tox dist

sdist:
	python3 setup.py -q sdist

checkver:
	@echo "Checking version"
	@grep -Eq '^\w+ v$(RXVERSION)\b' $(NEWS) \
	|| { echo "Version '$(VERSION)' not in $(NEWS)"; exit 1; }
	@echo "Checking git repo"
	@git diff --stat --exit-code || { echo "ERROR: Unclean repo"; exit 1; }

release: checkver
	git tag $(TAG)
	git push github $(TAG):$(TAG)

unrelease:
	git push github :$(TAG)
	git tag -d $(TAG)

shownote:
	awk -v VER="v$(VERSION)" -f etc/note.awk $(NEWS) \
	| pandoc -f rst -t gfm --wrap=none

#
# docker tests
#

pull-python:
	docker pull python:3.11-slim-bookworm
	docker pull python:3.10-slim-bookworm
	docker pull python:3.9-slim-bookworm
	docker pull python:3.8-slim-bookworm
	docker pull python:3.7-slim-bookworm

pull-postgres:
	docker pull postgres:15-bookworm
	docker pull postgres:14-bookworm
	docker pull postgres:13-bookworm
	docker pull postgres:12-bookworm
	docker pull postgres:11-bookworm

prune:
	docker image prune -f
	docker image ls

#
# test with combined image
#

COMPOSE_COMBO = docker compose -f etc/compose-combo.yml --project-directory .

test-pg15-build:
	$(COMPOSE_COMBO) build test-pg15

test-pg15-shell:
	$(COMPOSE_COMBO) run --entrypoint bash test-pg15

test-pg10 test-pg11 test-pg12 test-pg13 test-pg14 test-pg15:
	$(COMPOSE_COMBO) up --build $@

#
# does not work yet
#

COMPOSE_SPLIT = docker compose -f etc/compose-split.yml --project-directory .

dtest-db-build:
	$(COMPOSE_SPLIT) build db

dtest-db-shell:
	$(COMPOSE_SPLIT) run --entrypoint bash db

dtest-db-up:
	$(COMPOSE_SPLIT) up --build db

dtest-worker-build:
	$(COMPOSE_SPLIT) build test

dtest-worker-shell:
	$(COMPOSE_SPLIT) run --entrypoint bash test

dtest-worker-up:
	$(COMPOSE_SPLIT) up --build test

dtest-split-run:
	$(COMPOSE_SPLIT) up

