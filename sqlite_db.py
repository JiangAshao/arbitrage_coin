# -*- coding: utf-8 -*-
# @Time    : 2021-06-08 21:10
# @Author  : Ashao

import sqlite3


def sqlite_init():
    data_base = sqlite3.connect('arbitrage_coin.db')
    cursor = data_base.cursor()
    cursor.execute("")
    data_base.commit()
    data_base.close()


def sqlite_conn():
    conn = sqlite3.connect('arbitrage_coin.db')
    cur = conn.cursor()
    return conn, cur


def sqlite_execute(sql, conn, cur):
    cur.execute(sql)
    conn.commit()
    return cur.fetchall()


def sqlite_close(db):
    db.close()
