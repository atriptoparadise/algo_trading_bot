import logging
import datetime
import os
from functools import wraps

def logger_setup():
	os.makedirs("logs", exist_ok=True)
	logger = logging.getLogger(__name__)

	file_handler = logging.FileHandler('Signal{:%Y-%m-%d}.log'.format(datetime.now()))
	formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s \n")
	stream_handle = logging.StreamHandler()
	logging.basicConfig(level=logging.INFO, format=formatter, handlers=[file_handler, stream_handle])
	return logger


class FuncTimer(object):
	def __init__(self):
		self.cache = {}
	def timer_decorator(self, func):
		@wraps(func)
		def timer(*args, **kwargs):
			start = datetime.datetime.now()
			try:
				return func(*args, **kwargs)
			finally:
				end = datetime.datetime.now()
				if func.__name__ in self.cache:
					self.cache[func.__name__] += end-start
				else:
					self.cache[func.__name__] = end-start
		return timer

	def get_times(self):
		for k, v in self.cache.items():
			print (f"{k} execution time: {v}")
		self.cache = {}

