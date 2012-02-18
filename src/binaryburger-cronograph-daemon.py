#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
binaryburger-cronmanager-daemon.py: Daemon to execute tasks managed by the BinaryBurger CronManager

Author: Jens Nistler <loci@binaryburger.com>
License: GPL
Version: 1.0
"""

import argparse, daemon, daemon.pidfile, os, sys, urllib, urllib2, base64, json, logging, subprocess, shlex
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
	command = None
	shell = None
	max_time = 0
	date = None
	process = None

	def __init__(self, id, user, shell, command, max_time=0):
		"""Set data for thread execution
		"""

		Thread.__init__(self)
		self.id = id
		self.shell = shell
		self.command = str(command)
		self.max_time = int(max_time)
		self.user = str(user)

	def set_api_uri(self, uri):
		self.uri = uri

	def run(self):
		if self.send_start() is False:
			return
		if self.execute_task() is False:
			return
		self.send_stop()

	def send_start(self):
		"""Call API to report start of execution
		"""

		logging.debug("Sending start of task execution " + str(self.id))

		data = self.make_request("task/execution/" + str(self.id) + '/start', "PUT")
		if data is False or not data['id'] or data['id'] != self.id:
			logging.error("Failed to send start of task execution " + str(self.id))
			return False

		return True

	def execute_task(self):
		"""Execute the task in a subprocess
		"""

		logging.debug("Starting to process task execution " + str(self.id))

		self.date = datetime.utcnow()
		try:
			userdata = getpwnam(self.user)
			def result():
				os.setgid(userdata.pw_uid)
				logging.debug("Executing task as " + self.user + " with uid " + str(userdata.pw_uid))

			self.process = subprocess.Popen(
				shlex.split(self.command),
				executable=self.shell,
				stdout=subprocess.PIPE,
				shell=True,
				preexec_fn=result
			)
			return True
		except OSError, e:
			error_message = "Failed to execute command '" + self.command + "'"
			if hasattr(e, "strerror") and isinstance(e.strerror, str):
				error_message += " (" + e.strerror + ")"
			logging.error(error_message)

		return False

	def send_stop(self):
		"""Wait for task to finish or kill it if max execution time is reached.
		Report end of execution to API
		"""

		logging.debug("Waiting for task execution " + str(self.id))

		duration = 0
		while True:
			self.process.poll()
			durationDelta = (datetime.utcnow() - self.date)
			duration = float(durationDelta.days * 24 * 60 * 60) + float(durationDelta.seconds) + (float(durationDelta.microseconds) / 1000000)

			# we're done
			if self.process.returncode is not None:
				logging.debug("Finished task execution " + str(self.id) + " in " + str(duration) + " seconds")
				break

			# no runtime limit or not yet reached
			if self.max_time == 0 or duration < self.max_time:
				sleep(0.1)
				continue

			# runtime limit reached, kill the task
			try:
				self.process.kill()
				while self.process.returncode is None:
					self.process.poll()
					sleep(0.1)
				logging.debug("Terminated task execution " + str(self.id))
				break
			except IOError:
				logging.error("Failed to terminate task execution " + str(self.id))

		logging.debug("Sending end of task execution " + str(self.id))

		data = self.make_request("task/execution/" + str(self.id) + "/end", "PUT", {
			"return_code": self.process.returncode,
			"duration": duration,
			"output": self.process.stdout.read()
		})

		if data is False or not data['id'] or data['id'] != self.id:
			logging.error("Failed to send end of task execution " + str(self.id))
			return False

		return True


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
		logging.basicConfig(
			level=log_level,
			format = "%(asctime)s [%(levelname)-8s] (" + args.Server + ") %(message)s",
			datefmt = "%Y-%m-%d %H:%M:%S",
			filename = args.LogFile
		)

		# set offset
		self.offset_seconds = randint(0, 59)
		logging.debug("Call offset set to " + str(self.offset_seconds) + " seconds")

		# use another api - just for development
		if args.API:
			self.uri = args.API
			logging.debug('Switched API to ' + self.uri)

		self.PidFile = args.PidFile

		if args.Start is True and args.Stop is True:
			logging.critical("You cannot start and stop the daemon at the same time")
			return

		# validate credentials
		if self.validate_credentials(args.Server, args.Secret) is not True:
			logging.critical("Invalid credentials")
			return

		if args.Start is True:
			self.start_daemon()
			return

		if args.Stop is True:
			self.stop_daemon()
			return

		logging.critical("Shall I --start or --stop the daemon?")

	def start_daemon(self):
		"""Handle daemon startup and daemonization
		"""

		logging.info("Starting Cron-o-graph daemon...")

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
			}
		)

		with self.Daemon:
			logging.debug("Daemonizing...")
			self.loop()

	def stop_daemon(self):
		if not os.path.exists(self.PidFile):
			logging.critical("Daemon not running or invalid pidfile")
			return

		pid = open(self.PidFile).read()
		logging.info("Stopping Cron-o-graph daemon...")
		os.kill(int(pid), SIGTERM)

	def loop(self):
		while True:
			task_list = self.make_request("ping")
			if task_list is False:
				logging.error("Failed to get tasks from server")
			elif not len(task_list):
				logging.error("No tasks to process")
			else:
				for task in task_list:
					agent = cronograph_agent(task["id"], task["user"], task["shell"], task["command"], task["max_time"])
					agent.set_credentials(self.server, self.secret)
					agent.set_api_uri(self.uri)
					agent.start()

			# wait until next full minute
			sleep_period = self.get_sleep_period()
			logging.debug("Going to sleep for " + str(sleep_period) + " seconds")
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
