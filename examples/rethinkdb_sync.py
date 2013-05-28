#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Insert a new element in  a RethinkDB database
# when an evenement is trigger in MySQL replication log
#
# Please test with MySQL employees DB available here:
# https://launchpad.net/test-db/
#

import rethinkdb

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": ""
}


def main():
    # connect rethinkdb
    rethinkdb.connect("localhost", 28015, "mysql")
    try:
        rethinkdb.db_drop("mysql").run()
    except:
        pass
    rethinkdb.db_create("mysql").run()

    tables = ["dept_emp", "dept_manager", "titles",
              "salaries", "employees", "departments"]
    for table in tables:
        rethinkdb.db("mysql").table_create(table).run()

<<<<<<< HEAD
#MySQL
mysql_settings = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': ''}
__stream = BinLogStreamReader(connection_settings = mysql_settings,
                           only_events = [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent], blocking = True)

#Process Feed
for binlogevent in __stream:
    prefix = "%s:%s:" % (binlogevent.schema, binlogevent.table)
=======
    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        blocking=True,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
    )

    # process Feed
    for binlogevent in stream:
        if not isinstance(binlogevent, WriteRowsEvent):
            continue
>>>>>>> 7e582fb4e8a3a5ea212f15e7558cb6da43c37e2c

        for row in binlogevent.rows:
            if not binlogevent.schema == "employees":
                continue

<<<<<<< HEAD
__stream.close()
=======
            vals = {}
            vals = {str(k): str(v) for k, v in row["values"].iteritems()}
            rethinkdb.table(binlogevent.table).insert(vals).run()
>>>>>>> 7e582fb4e8a3a5ea212f15e7558cb6da43c37e2c

    stream.close()


if __name__ == "__main__":
    main()
