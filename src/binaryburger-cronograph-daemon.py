#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
binaryburger-cronmanager-daemon.py: Daemon to execute tasks managed by the BinaryBurger CronManager

Author: Jens Nistler <loci@binaryburger.com>
License: GPL
Version: 1.0
"""

import argparse, daemon, daemon.pidfile, os, sys, urllib, urllib2, base64, json, logging, subprocess
from signal import SIGTERM
from time import sleep
from datetime import datetime, timedelta
from threading import Thread
from pwd import getpwnam
from random import randint

class http_request(urllib2.Request):
	"""Custom HTTP request handler to ease the use of urllib2
	"""

	# current request method
	method = "GET"

	def set_method(self, method):
		"""Set HTTP method for next request
		"""

		if method not in ("GET", "POST", "PUT", "DELETE", "OPTIONS"):
			raise urllib2.HTTPError

		self.method = method

	def get_method(self):
		"""Get HTTP method for next request
		"""

		return self.method

	def set_auth(self, user, password):
		"""Set username and password for http basic authentication
		"""

		self.add_header("Authorization", "Basic %s" % base64.encodestring("%s:%s" % (user, password))[:-1])

	def set_data(self, data):
		"""Set request data
		"""

		if data is not None:
			data = urllib.urlencode(data)

		self.data = data


class cronograph_base:
	"""Base class for manager and daemon.
	Verifies API credentials and handles request errors
	"""

	uri = "http://www.binaryburger.com/cronograph/api/"
	server = None
	secret = None
	logger = None

	def validate_credentials(self, server, secret):
		"""Validate API credentials
		"""

		self.set_credentials(server, secret)

		data = self.make_request("validate/credentials")
		if data is True:
			return True

		return False

	def set_credentials(self, server, secret):
		"""Set API credentials
		"""

		self.server = server
		self.secret = secret

	def make_request(self, command, method="GET", post_data=None):
		"""Send API request and handle errors
		"""

		request = http_request(self.uri + command)
		request.set_auth(self.server, self.secret)
		request.set_method(method)
		request.set_data(post_data)

		try:
			logging.debug("HTTP " + request.get_method() + " request: " + request.get_full_url())
			response_handle = urllib2.urlopen(request)
			response_json = json.loads(response_handle.read())
			return response_json
		except IOError, e:
			error_message = "Failed to get response from server"
			if hasattr(e, "code") and e.code != 0:
				error_message += " (" + str(e.code) + ")"
			logging.error(error_message)
			if post_data is not None:
				logging.error(post_data)
		except ValueError, e:
			error_message = "Failed to decode server response"
			if hasattr(e, "message") and e.message.strip():
				error_message += " (" + e.message + ")"
			logging.error(error_message)

		return False


class cronograph_agent(Thread, cronograph_base):
	"""Thread to handle task execution
	"""

	id = None
	user = None
	shell = None
	command = None
	max_time = 0

	def __init__(self, id, user, shell, command, max_time=0):
		"""Set data for thread execution
		"""

		Thread.__init__(self)
		self.id = id
		self.user = str(user)
		self.shell = shell
		self.command = str(command)
		self.max_time = int(max_time)

	def set_api_uri(self, uri):
		self.uri = uri

	def set_logger(self, logger):
		self.logger = logger

	def run(self):
		self.logger.debug("Sending start of task execution " + str(self.id))

		data = self.make_request("task/execution/" + str(self.id) + '/start', "PUT")
		if data is False or not data['id'] or data['id'] != self.id:
			self.logger.error("Failed to send start of task execution " + str(self.id))
			return

		executionStart = datetime.utcnow()
		try:
			userdata = getpwnam(self.user)
			self.logger.debug("Executing task as " + self.user + " with uid " + str(userdata.pw_uid))

			def result():
				os.setgid(userdata.pw_uid)

			process = subprocess.Popen(
				self.command,
				executable=self.shell,
				shell=True,
				stdout=subprocess.PIPE,
				stderr=subprocess.PIPE,
				preexec_fn=result
			)
		except OSError, e:
			error_message = "Failed to execute command '" + self.command + "'"
			if hasattr(e, "strerror") and isinstance(e.strerror, str):
				error_message += " (" + e.strerror + ")"
			self.logger.error(error_message)
			return

		self.logger.debug("Waiting for task execution " + str(self.id))
		while process.poll() is None:
			# calculate time spent
			durationDelta = datetime.utcnow() - executionStart
			duration = float(int(durationDelta.days) * 24 * 60 * 60) + float(durationDelta.seconds) + (float(durationDelta.microseconds) / 1000000)

			# no runtime limit or not yet reached
			if self.max_time == 0 or duration < self.max_time:
				sleep(0.1)
			# runtime limit reached, kill the task
			else:
				try:
					process.kill()
					process.wait()
					self.logger.debug("Terminated task execution " + str(self.id))
					break
				except IOError:
					self.logger.error("Failed to terminate task execution " + str(self.id))

		# calculate time spent
		durationDelta = datetime.utcnow() - executionStart
		duration = float(int(durationDelta.days) * 24 * 60 * 60) + float(durationDelta.seconds) + (float(durationDelta.microseconds) / 1000000)
		self.logger.debug("Finished execution of task " + str(self.id) + " in " + str(duration) + " seconds with return code " + str(process.returncode))

		self.logger.debug("Sending end of task execution " + str(self.id))
		data = self.make_request("task/execution/" + str(self.id) + "/end", "PUT", {
			"return_code": process.returncode,
			"duration": duration,
			"output": self.split_utf8(process.stdout.read(), 4 * 1024),
			"error": self.split_utf8(process.stderr.read(), 4 * 1024),
		})

		if data is False or not data['id'] or data['id'] != self.id:
			self.logger.error("Failed to send end of task execution " + str(self.id))

	def split_utf8(self, string, bytes):
		if len(string) <= bytes:
			return string
		while 0x80 <= ord(string[bytes]) < 0xc0:
			bytes -= 1
		return string[0:bytes]


class cronograph_daemon(cronograph_base):
	offset_seconds = 0
	PidFile = None
	Daemon = None
	Workers = {}

	def __init__(self):
		"""Parse command line arguments and start the daemon
		"""

		parser = argparse.ArgumentParser(
			description="BinaryBurger Cron-o-graph daemon"
		)
		parser.add_argument(
			"--start",
			dest="Start",
			action="store_true",
			required=False,
		)
		parser.add_argument(
			"--stop",
			dest="Stop",
			action="store_true",
			required=False,
		)
		parser.add_argument(
			"--verbose",
			dest="Verbose",
			action="store_true",
			required=False,
		)
		parser.add_argument(
			"--server",
			dest="Server",
			help="The server name as shown on the Cron-o-graph web interface",
			required=True
		)
		parser.add_argument(
			"--secret",
			dest="Secret",
			help="The server secret as shown on the Cron-o-graph web interface",
			required=True
		)
		parser.add_argument(
			"--pid",
			dest="PidFile",
			help="The location for the Cron-o-graph pidfile",
			required=False,
			default="./cronograph.pid"
		)
		parser.add_argument(
			"--log",
			dest="LogFile",
			help="The location for the Cron-o-graph logfile",
			required=False,
			default="/var/log/cronograph.log"
		)
		parser.add_argument(
			"--api",
			dest="API",
			help="Set different API URI",
			required=False,
			default="http://www.binaryburger.com/cronograph/api/"
		)
		args = parser.parse_args()

		# check if logfile is writable
		fileExistsNotWriteable = os.path.exists(args.LogFile) and not os.access(args.LogFile, os.W_OK)
		fileNotExistsPathNotWriteable = not os.path.exists(args.LogFile) and not os.access(os.path.dirname(args.LogFile), os.W_OK)

		if fileExistsNotWriteable or fileNotExistsPathNotWriteable:
			print "Logfile is not writeable: " + args.LogFile
			return

		# set log verbosity
		if args.Verbose is True:
			log_level = logging.DEBUG
		else:
			log_level = logging.ERROR
		self.logger = logging.getLogger("cronograph")
		self.logger.setLevel(log_level)
		logFormatter = logging.Formatter("%(asctime)s [%(levelname)-8s] (" + args.Server + ") %(message)s", "%Y-%m-%d %H:%M:%S")
		logHandler = logging.FileHandler(args.LogFile)
		logHandler.setFormatter(logFormatter)
		self.logger.addHandler(logHandler)

		# set offset
		self.offset_seconds = randint(0, 59)
		self.logger.debug("Call offset set to " + str(self.offset_seconds) + " seconds")

		# use another api - just for development
		if args.API:
			self.uri = args.API
			self.logger.debug('Switched API to ' + self.uri)

		self.PidFile = args.PidFile

		if args.Start is True and args.Stop is True:
			self.logger.critical("You cannot start and stop the daemon at the same time")
			return

		# validate credentials
		if self.validate_credentials(args.Server, args.Secret) is not True:
			self.logger.critical("Invalid credentials")
			return

		if args.Start is True:
			self.start_daemon(logHandler.stream)
			return

		if args.Stop is True:
			self.stop_daemon()
			return

		self.logger.critical("Shall I --start or --stop the daemon?")

	def start_daemon(self, logHandlerStream):
		"""Handle daemon startup and daemonization
		"""

		self.logger.info("Starting Cron-o-graph daemon...")

		if os.path.exists(self.PidFile):
			print "Daemon already running, pid file " + self.PidFile + " exists"
			return

		pid = daemon.pidfile.TimeoutPIDLockFile(
			self.PidFile,
			10
		)

		self.Daemon = daemon.DaemonContext(
			uid=os.getuid(),
			gid=os.getgid(),
			pidfile=pid,
			working_directory=os.getcwd(),
			detach_process=True,
			signal_map={
				SIGTERM: self.terminate,
			},
			files_preserve=[
				logHandlerStream
			]
		)

		with self.Daemon:
			self.logger.debug("Daemonizing...")
			self.loop()

	def stop_daemon(self):
		if not os.path.exists(self.PidFile):
			self.logger.critical("Daemon not running or invalid pidfile")
			return

		pid = open(self.PidFile).read()
		self.logger.info("Stopping Cron-o-graph daemon...")
		os.kill(int(pid), SIGTERM)

	def loop(self):
		while True:
			task_list = self.make_request("ping")
			if task_list is False:
				self.logger.error("Failed to get tasks from server")
			elif not len(task_list):
				self.logger.error("No tasks to process")
			else:
				for task in task_list:
					agent = cronograph_agent(task["id"], task["user"], task["shell"], task["command"], task["max_time"])
					agent.set_credentials(self.server, self.secret)
					agent.set_api_uri(self.uri)
					agent.set_logger(self.logger)
					agent.start()

			# wait until next full minute
			sleep_period = self.get_sleep_period()
			self.logger.debug("Going to sleep for " + str(sleep_period) + " seconds")
			sleep(int(sleep_period))

	def get_sleep_period(self):
		"""Calculate time until the next full minute
		"""

		utc_now = datetime.utcnow()
		sleep_until = utc_now + timedelta(minutes=1)
		sleep_until = sleep_until.replace(second=self.offset_seconds)
		sleep_period = (sleep_until - utc_now).seconds
		return sleep_period

	def terminate(self, signal, action):
		"""Signal handler for daemon shutdown
		"""

		self.Daemon.close()
		sys.exit(0)


# start me up!
if __name__ == "__main__":
	daemon = cronograph_daemon()
