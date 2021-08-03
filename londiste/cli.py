"""Londiste launcher.
"""

import optparse
import sys

import skytools

import pgq.cascade.admin
from londiste.admin import LondisteSetup
from londiste.compare import Comparator
from londiste.playback import Replicator
from londiste.repair import Repairer
from londiste.table_copy import CopyTable

command_usage = pgq.cascade.admin.command_usage + """
Replication Daemon:
  worker                replay events to subscriber

Replication Administration:
  add-table TBL ...     add table to queue
  remove-table TBL ...  remove table from queue
  change-handler TBL    change handler for the table
  add-seq SEQ ...       add sequence to provider
  remove-seq SEQ ...    remove sequence from provider
  tables                show all tables on provider
  seqs                  show all sequences on provider
  missing               list tables subscriber has not yet attached to
  resync TBL ...        do full copy again
  wait-sync             wait until all tables are in sync

Replication Extra:
  check                 compare table structure on both sides
  fkeys                 print out fkey drop/create commands
  compare [TBL ...]     compare table contents on both sides
  repair [TBL ...]      repair data on subscriber
  execute [FILE ...]    execute SQL files on set
  show-handlers [..]    show info about all or specific handler

Internal Commands:
  copy                  copy table logic
"""

cmd_handlers = (
    (('create-root', 'create-branch', 'create-leaf', 'members', 'tag-dead', 'tag-alive',
      'change-provider', 'rename-node', 'status', 'node-status', 'pause', 'resume', 'node-info',
      'drop-node', 'takeover', 'resurrect'), LondisteSetup),
    (('add-table', 'remove-table', 'change-handler', 'add-seq', 'remove-seq', 'tables', 'seqs',
      'missing', 'resync', 'wait-sync', 'wait-root', 'wait-provider',
      'check', 'fkeys', 'execute'), LondisteSetup),
    (('show-handlers',), LondisteSetup),
    (('worker',), Replicator),
    (('compare',), Comparator),
    (('repair',), Repairer),
    (('copy',), CopyTable),
)


class Londiste(skytools.DBScript):

    script: skytools.DBScript

    def __init__(self, args):
        self.full_args = args

        super().__init__('londiste', args)

        if len(self.args) < 2:
            print("need command")
            sys.exit(1)
        cmd = self.args[1]
        script = None
        for names, cls in cmd_handlers:
            if cmd in names:
                script = cls(args)
                break
        if not script:
            print("Unknown command '%s', use --help for help" % cmd)
            sys.exit(1)
        self.script = script

    def start(self):
        self.script.start()

    def print_ini(self):
        """Let the Replicator print the default config."""
        Replicator(self.full_args)

    def init_optparse(self, parser=None):
        p = super().init_optparse(parser)
        p.set_usage(command_usage.strip())

        g = optparse.OptionGroup(p, "options for cascading")
        g.add_option("--provider",
                     help="init: upstream node temp connect string")
        g.add_option("--target", metavar="NODE",
                     help="switchover: target node")
        g.add_option("--merge", metavar="QUEUE",
                     help="create-leaf: combined queue name")
        g.add_option("--dead", metavar="NODE", action='append',
                     help="cascade: assume node is dead")
        g.add_option("--dead-root", action='store_true',
                     help="takeover: old node was root")
        g.add_option("--nocheck", action='store_true',
                     help="create: skip public connect string check")
        g.add_option("--dead-branch", action='store_true',
                     help="takeover: old node was branch")
        g.add_option("--sync-watermark", metavar="NODES",
                     help="create-branch: list of node names to sync wm with")
        p.add_option_group(g)

        g = optparse.OptionGroup(p, "repair queue position")
        g.add_option("--rewind", action="store_true",
                     help="change queue position according to destination")
        g.add_option("--reset", action="store_true",
                     help="reset queue position on destination side")
        p.add_option_group(g)

        g = optparse.OptionGroup(p, "options for add")
        g.add_option("--all", action="store_true",
                     help="add: include all possible tables")
        g.add_option("--wait-sync", action="store_true",
                     help="add: wait until all tables are in sync")
        g.add_option("--dest-table", metavar="NAME",
                     help="add: redirect changes to different table")
        g.add_option("--expect-sync", action="store_true", dest="expect_sync",
                     help="add: no copy needed", default=False)
        g.add_option("--skip-truncate", action="store_true", dest="skip_truncate",
                     help="add: keep old data", default=False)
        g.add_option("--create", action="store_true",
                     help="add: create table/seq if not exist, with minimal schema")
        g.add_option("--create-full", action="store_true",
                     help="add: create table/seq if not exist, with full schema")
        g.add_option("--trigger-flags",
                     help="add: set trigger flags (BAIUDLQ)")
        g.add_option("--trigger-arg", action="append",
                     help="add: custom trigger arg (can be specified multiple times)")
        g.add_option("--no-triggers", action="store_true",
                     help="add: do not put triggers on table (makes sense on leaf)")
        g.add_option("--handler", action="store",
                     help="add: custom handler for table")
        g.add_option("--handler-arg", action="append",
                     help="add: argument to custom handler")
        g.add_option("--find-copy-node", dest="find_copy_node", action="store_true",
                     help="add: walk upstream to find node to copy from")
        g.add_option("--copy-node", metavar="NODE", dest="copy_node",
                     help="add: use NODE as source for initial COPY")
        g.add_option("--merge-all", action="store_true",
                     help="merge tables from all source queues", default=False)
        g.add_option("--no-merge", action="store_true",
                     help="don't merge tables from source queues", default=False)
        g.add_option("--max-parallel-copy", metavar="NUM", type="int",
                     help="max number of parallel copy processes")
        g.add_option("--skip-non-existing", action="store_true",
                     help="add: skip object that does not exist")
        p.add_option_group(g)

        g = optparse.OptionGroup(p, "options for tables")
        g.add_option("--names-only", action="store_true",
                     help="tables: show only table names (for scripting)")
        p.add_option_group(g)

        g = optparse.OptionGroup(p, "other options")
        g.add_option("--force", action="store_true",
                     help="add: ignore table differences, repair: ignore lag")
        g.add_option("--apply", action="store_true",
                     help="repair: apply fixes automatically")
        g.add_option("--count-only", action="store_true",
                     help="compare: just count rows, do not compare data")
        g.add_option("--sort-bufsize", action="store",
                     help="repair: set coreutils sort bufsize (default: 30%)")
        g.add_option("--repair-where", action="store",
                     help="repair: use where condition to filter rows for repair")
        p.add_option_group(g)

        return p


def main():
    script = Londiste(sys.argv[1:])
    script.start()


if __name__ == '__main__':
    main()

