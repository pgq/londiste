
NEWS
====

Londiste v3.12
--------------

* typing: add full typing
* tests/obfuscate: fix pgqd conf
* build: convert to pyproject
* mk: test-db15 .. test-pg15, test-all targets for docker tests
* ci: drop obsolete actions
* handlers: drop encoding validator, it has never worked on Py3

Londiste v3.11
--------------

* playback: allow EXECUTE to pass through on merge nodes
* playback: make EXECUTE passthrough tunable
* tox: update packages
* cleanup: improve typings

Londiste v3.10
--------------

* shard: add disable_replay parameter.
* lint: upgrade linters, fix new warnings

Londiste v3.9.2
---------------

* playback: fix weird result check

Londiste v3.9.1
---------------

* playback: tolerate weird result from version query

Londiste v3.9
-------------

* playback: support multistep fkey restore

Londiste v3.8.6
---------------

* playback: fix variable init.

Londiste v3.8.5
---------------

* playback: move ``local_only`` setup even earlier.

Londiste v3.8.4
---------------

* playback: fix ``local_only`` setup which allowed first batch without filter.

Londiste v3.8.3
---------------

* status: support --compact option

Londiste v3.8.2
---------------

* shard: better error handling on missing shard key
* admin: disable pidfile write for wait-sync

Londiste v3.8.1
---------------

* Filter tables on registration: register_only_tables/register_skip_tables
* Filter seqs on registration: register_only_seqs/register_skip_seqs

Londiste v3.8
-------------

* shard handler: support filtered copy, load settings from config file:
  ``shard_hash_func``, ``shard_info_sql``.
* fix: always call handler's ``prepare_batch``.

Londiste v3.7.1
---------------

* Fix write_hook in parallel copy.

Londiste v3.7
-------------

* Parallel single table copy:

  - threaded_copy_tables - list of glob patterns for table names
  - threaded_copy_pool_size - number of threads

* Various linter fixes

Londiste v3.6.1
---------------

* Fix fkey log message
* Upgrade Skytools dependency in tox to get copy_from fix
* Various linter fixes

Londiste v3.6
-------------

* obfuscate: process copy events
* Various linter fixes
* Docker tests

Londiste v3.5
-------------

* obfuscate: Improved decoding/encoding handling
* Setup Github Actions
* Code cleanups
* Drop Debian packaging

Londiste v3.4.1
---------------

* obfuscate: improvements for better usage with inheritance
* pip: due to psycopg2/psycopg2-binary duality, drop direct dependency

Londiste v3.4
-------------

* Move to separate repo

