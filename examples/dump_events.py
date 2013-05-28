#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Dump all replication events from a remote mysql server
#

from pymysqlreplication import BinLogStreamReader

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": ""
}


<<<<<<< HEAD
#server_id is your slave identifier. It should be unique
#blocking: True if you want to block and wait for the next event at the end of the stream
__stream = BinLogStreamReader(connection_settings = mysql_settings, server_id = 3, blocking = True)

for binlogevent in __stream:
    binlogevent.dump()

__stream.close()
=======
def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=3,
                                blocking=True)

    for binlogevent in stream:
        binlogevent.dump()

    stream.close()
>>>>>>> 7e582fb4e8a3a5ea212f15e7558cb6da43c37e2c


if __name__ == "__main__":
    main()
