#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
DbStatusHandler: handler that inserts object ids into the database for saving state
'''

import logging

logger = logging.getLogger(__name__)

from .base_handler import BaseHandler
import multiprocessing as mp
import futures, json, copy, time
from tweetf0rm.redis_helper import NodeQueue, NodeCoordinator, RedisQueue
from tweetf0rm.utils import full_stack, get_keys_by_min_value, hash_cmd, get_status_queue_name
import json


class DbStatusHandler(BaseHandler):

	def __init__(self, redis_config = None):
		'''
		Stores ids into the database
		'''
		super(DbStatusHandler, self).__init__()
		self.redis_config = redis_config
		self.status_queue = RedisQueue(name=get_status_queue_name(), queue_type='fifo', redis_config=self.redis_config)

	def need_flush(self, bucket):
		# flush every time there is new data comes in
		return True

	def flush(self, bucket):
		logger.debug("i'm getting flushed...")

		for k, v in self.buffer[bucket].iteritems():
			for s in v:
				o = json.loads(s)

				self.status_queue.put(o)
		
		# send to a different process to operate, clear the buffer
		self.clear(bucket)

		True