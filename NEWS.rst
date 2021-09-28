
NEWS
====

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

