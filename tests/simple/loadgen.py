#! /usr/bin/env python3

import sys

import skytools


class LoadGen(skytools.DBScript):
    seq = 1
    def work(self):
        db = self.get_database('db', autocommit=1)
        curs = db.cursor()
        data = 'data %d' % self.seq
        curs.execute('insert into mytable (data) values (%s)', [data])
        self.seq += 1


if __name__ == '__main__':
    script = LoadGen('loadgen', sys.argv[1:])
    script.start()

