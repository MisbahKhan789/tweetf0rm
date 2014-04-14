#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)

import sqlite3

def buildTable(conn):

	c = conn.cursor()

	c.execute("CREATE TABLE IF NOT EXISTS fetched_ids (obj_id INT, status INT)")

def connect_to_db(dbPath):

	conn = sqlite3.connect(dbPath)

	buildTable(conn)

	return conn

def id_exists(objId, cursor):

	t = (objId,)
	cursor.execute('SELECT EXISTS(SELECT 1 FROM fetched_ids WHERE obj_id=? LIMIT 1);', t)
	
	retVal = False

	cursorRet = cursor.fetchone()
	if ( cursorRet[0] == 1):
		retVal = True

	return retVal

def update_id_status(objId, status, cursor):

	t = (status, objId)
	cursor.execute('UPDATE fetched_ids SET status=? WHERE obj_id=?;', t)

def insert_id_status(objId, status, cursor):

	t = (objId,status)
	cursor.execute('INSERT INTO fetched_ids (obj_id, status) VALUES (?, ?)', t)

def get_id_status(objId, cursor):

	if ( id_exists(objId, cursor) == False ):
		return 0

	t = (objId,)
	cursor.execute('SELECT status FROM fetched_ids WHERE obj_id=?;', t)

	cursorRet = cursor.fetchone()

	return cursorRet[0]