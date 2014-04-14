#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s-[%(asctime)s][%(module)s][%(funcName)s][%(lineno)d]: %(message)s')
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

import sys, time, argparse, json, os, pprint
sys.path.append(".")

import multiprocessing as mp
from tweetf0rm.exceptions import InvalidConfig
from tweetf0rm.redis_helper import NodeQueue, NodeCoordinator, RedisQueue
from tweetf0rm.utils import full_stack, node_id, public_ip, get_status_queue_name
from tweetf0rm.proxies import proxy_checker
from tweetf0rm.scheduler import Scheduler
from tweetf0rm.sqlite import connect_to_db, id_exists, update_id_status, insert_id_status
import time, os, tarfile, futures

def check_config(config):
	if ('apikeys' not in config or 'redis_config' not in config):
		raise InvalidConfig("something is wrong with your config file... you have to have redis_config and apikeys")

def tarball_results(data_folder, bucket, output_tarball_foldler, timestamp):

	logger.info("archiving bucket: [%s] at %s"%(bucket, timestamp))
	data_folder = os.path.join(os.path.abspath(data_folder), bucket)

	if (not os.path.exists(data_folder)):
		os.makedirs(data_folder)

	output_tarball_foldler = os.path.join(os.path.abspath(output_tarball_foldler), bucket)

	if (not os.path.exists(output_tarball_foldler)):
		os.makedirs(output_tarball_foldler)

	gz_file = os.path.join(output_tarball_foldler, '%s.tar.gz'%timestamp) 
	ll = []
	
	ignores = ['.DS_Store']
	for root, dirs, files in os.walk(data_folder):
		if (len(files) > 0):
			with tarfile.open(gz_file, "w:gz") as tar:
				cnt = 0
				for f in files:
					if (f in ignores):
						continue
					f_abspath = os.path.join(root, f)
					(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(f_abspath)

					if (mtime <= timestamp):
						tar.add(f_abspath, '%s/%s'%(timestamp,f), recursive=False)
						ll.append(f_abspath)
						cnt += 1
						if (cnt % 1000 == 0):
							logger.info("processed %d files"%(cnt))
					else:
						pass
				#logger.debug(time.ctime(atime))


				tar.close()

				for f in ll:

					os.remove(f)

				return True, gz_file

	return False, gz_file

def check_status_db(status_queue, db):

	tweet_obj_arr = []

	while ( status_queue.empty() == False ):

		tweet_id_obj = status_queue.get(block=False)

		if ( tweet_id_obj != None ):
			# Push status to db
			tweet_obj_arr.append(tweet_id_obj)
	
	if ( len(tweet_obj_arr) > 0 ):		
		update_status_db(tweet_obj_arr, db)

def update_status_db(tweetObjArr, db):

	for tweetObj in tweetObjArr:
		tweetId = tweetObj['id']

		cursor = db.cursor()
		if ( id_exists(tweetId, cursor) == True ):
			update_id_status(tweetId, tweetObj['status'], cursor)
		else:
			insert_id_status(tweetId, tweetObj['status'], cursor)
		db.commit()


def start_server(config, proxies, db):
	import copy
	
	check_config(config)
	config = copy.copy(config)

	folders_to_create = []
	buckets = ["tweets", "followers", "follower_ids", "friends", "friend_ids", "timelines"]

	ouput_folder = os.path.abspath(config['output'])
	archive_output = os.path.abspath(config['archive_output']) if config['archive_output'] else ouput_folder
	archive_output = os.path.join(archive_output, 'archived')

	folders_to_create.append(ouput_folder)
	folders_to_create.append(archive_output)

	for bucket in buckets:
		folders_to_create.append(os.path.join(ouput_folder, bucket))
		folders_to_create.append(os.path.join(archive_output, bucket))

	for folder_to_create in folders_to_create:
		if (not os.path.exists(folder_to_create)):
			os.makedirs(folder_to_create)

	logger.info("output to %s"%(ouput_folder))
	logger.info("archived to %s"%(archive_output))

	this_node_id = node_id()
	node_queue = NodeQueue(this_node_id, redis_config=config['redis_config'])
	node_queue.clear()

	# This queue will hold state info we want to transfer to the state database
	status_queue = RedisQueue(name=get_status_queue_name(), queue_type='fifo', redis_config=config['redis_config'])
	status_queue.clear()

	scheduler = Scheduler(this_node_id, config=config, proxies=proxies)

	logger.info('starting node_id: %s'%this_node_id)

	node_coordinator = NodeCoordinator(config['redis_config'])
	#node_coordinator.clear()
	
	#the main event loop, actually we don't need one, since we can just join on the crawlers and don't stop until a terminate command is issued to each crawler;
	#but we need one to report the status of each crawler and perform the tarball tashs...
	
	last_archive_ts = time.time() + 3600 # the first archive event starts 2 hrs later... 
	last_status_ts = time.time() + 5 # Every n seconds, update the status database
	pre_time = time.time()
	last_load_balancing_task_ts = time.time()
	while True:
		
		if (time.time() - pre_time > 120):
			logger.info(pprint.pformat(scheduler.crawler_status()))
			pre_time = time.time()
			if (scheduler.is_alive()):
				cmd = {'cmd': 'CRAWLER_FLUSH'}
				scheduler.enqueue(cmd)

		if (time.time() - last_archive_ts > 3600):

			logger.info("start archive procedure...")
			with futures.ProcessPoolExecutor(max_workers=len(buckets)) as executor:

				future_proxies = {executor.submit(tarball_results, ouput_folder, bucket, archive_output, int(time.time()) - 3600): bucket for bucket in buckets}
		
				for future in future_proxies:
					future.add_done_callback(lambda f: logger.info("archive created? %s: [%s]"%f.result()))

			last_archive_ts = time.time()

		if ( time.time() - last_status_ts > 0 ):
			# update status database
			logger.info("Checking status queue...")
			check_status_db(status_queue, db)

			last_status_ts = time.time()

		# block, the main process...for a command
		if(not scheduler.is_alive()):
			logger.info("no crawler is alive... waiting to recreate all crawlers...")
			time.sleep(120) # sleep for a minute and retry
			continue

		if (time.time() - last_load_balancing_task_ts > 1800): # try to balance the local queues every 30 mins
			last_load_balancing_task_ts = time.time()
			cmd = {'cmd': 'BALANCING_LOAD'}
			scheduler.enqueue(cmd)

		cmd = node_queue.get(block=True, timeout=30)

		if cmd:
			scheduler.enqueue(cmd)
				

if __name__=="__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-c', '--config', help="config.json that contains a) twitter api keys; b) redis connection string;", required = True)
	parser.add_argument('-p', '--proxies', help="the proxies.json file")

	args = parser.parse_args()

	proxies = None
	if args.proxies:
		with open(os.path.abspath(args.proxies), 'rb') as proxy_f:
			proxies = json.load(proxy_f)['proxies']

	with open(os.path.abspath(args.config), 'rb') as config_f:
		config = json.load(config_f)	

		if ( config['db_path'] == None ):
			raise MissingArgs("you need a database path to write the state to...")
		
		try:
			db = connect_to_db(config['db_path'])
			start_server(config, proxies, db)
		except KeyboardInterrupt:
			print()
			logger.error('You pressed Ctrl+C!')
			pass
		except Exception as exc:		
			logger.error(exc)
			logger.error(full_stack())
		finally:
			pass