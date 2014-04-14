#!/usr/bin/python

import sqlite3
import sys
import json
import time
import subprocess

def getUnfetchedIds(db, limit=100):

	cursor = db.cursor()

	foundId = []

	sqlStr = 'SELECT obj_id FROM fetched_ids WHERE status=0 ORDER BY obj_id LIMIT %d;' % limit

	for row in cursor.execute(sqlStr):
		foundId.append(row[0])

	return foundId

def getPendingCount(db):

	cursor = db.cursor()

	foundId = []

	sqlStr = 'SELECT COUNT(obj_id) FROM fetched_ids WHERE status=2'

	count = 0
	for row in cursor.execute(sqlStr):
		count = row[0]

	return count

def writeIdsToJsonFile(idSet, path):

	outFile = open(path, 'w')

	outFile.write(json.dumps(idSet))

	outFile.close()

sqlitePath = sys.argv[1]

unCountList = []
unCountLimit = 100

runCount = 0

while True:

	runCount+=1

	print "Executing run: ", runCount

	db = sqlite3.connect(sqlitePath)

	unCount = getPendingCount(db)

	print "Currently see %d pending IDs..." % (unCount)

	unCountList.append(unCount)

	if unCount < unCountLimit:

		print "Fetching new tweets to request."

		unfetchedSet = getUnfetchedIds(db)
		db.close()

		print "Writing tweets to file."

		writeIdsToJsonFile(unfetchedSet, 'tmp_tweets.json')

		# call client w/ file
		output = subprocess.check_output("./client.sh -c config.json -cmd BATCH_CRAWL_TWEET -j tmp_tweets.json", shell=True)
		print output

	else:

		db.close()

		lastUnCount = unCountList[-4]

		print "Checking if last four pending counts are equal to %s..." % (unCountList[-4:])

		flag = False

		for v in unCountList[-4:]:
			if ( v != lastUnCount ):
				flag = True

		if ( flag ):
			print "We likely have not prcessed new tweets in the past 2 hours" % (lastUnCount)
			unCountLimit = lastUnCount + 100
			print "New limit: ", unCountLimit

			continue

	print "Sleeping..."
	# Sleep for 30 minutes
	time.sleep(60*30)