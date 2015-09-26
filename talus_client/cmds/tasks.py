#!/usr/bin/env python
# encoding: utf-8

import argparse
import cmd
import json
import os
import shlex
import sys
import time
from tabulate import tabulate

from talus_client.cmds import TalusCmdBase
import talus_client.api
import talus_client.errors as errors
from talus_client.models import Task

class TaskCmd(TalusCmdBase):
	"""The Talus task command processor
	"""

	command_name = "task"

	def do_list(self, args):
		"""List all tasks in
		"""
		tasks = []
		headers = ["id", "name", "tool", "version"]
		for task in self._talus_client.task_iter():
			tasks.append([
				task.id,
				task.name,
				task.tool + " (" + task._fields["tool"].value["name"] + ")",
				task.version
			])
		print(tabulate(tasks, headers=headers))
	
	def do_info(self, args):
		"""List details about a task
		"""

	def do_create(self, args):
		"""Create a new task in Talus

		create -n NAME -t TOOL_ID_OR_NAME -p PARAMS -l LIMIT

		        -n,--name    The name of the new task (required, no default)
		        -t,--tool    The name or id of the tool to be run by the task (required, no default)
		       -l,--limit    The limit for the task. What the limit means is defined by how the tool
			                 reports progress. If the tool does not report progress, then the limit
							 means the number of total VMs to run.
				 --vm-max    Maximum amount of time a vm should be allowed to run (defaults to 30m)
				             You may use values such as 30m15s. If no units are used, the value is
							 assumed to be in seconds.
		      -p,--params    The params of the task
			      --shell    Create the task in an interactive shell (default if already in shell and no args)
		     -v,--version    The version the task should be pinned at, else the current HEAD (default=None)
		 -f,--params-file    The file that contains the params of the task

		Examples:
		---------

		To create a new task that uses the tool "BrowserFuzzer":

		    task create -n "IE Fuzzer" -t "BrowserFuzzer" -p "{...json params...}"

		To create a new task that also uses the "BrowserFuzzer" tool but reads in the params
		from a file and has a max vm runtime of 45 minutes 10 seconds:

		    task create -n "IE Fuzzer" -t "BrowserFuzzer" -f ie_fuzz_params.json --vm-max 45m10s
		"""
		args = shlex.split(args)
		if self._go_interactive(args):
			task = Task()
			self._prep_model(task)
			task.version = None

			stop = False
			while not stop:
				model_cmd = self._make_model_cmd(task)
				cancelled = model_cmd.cmdloop()
				if cancelled:
					break

				stop = True
				if task.limit is None:
					self.err("You must set a default limit for the task!")
					stop = False

				if task.name is None or task.name == "":
					self.err("You must set a name for the task!")
					stop = False

				if task.tool is None:
					self.err("You must choose a tool for the task!")
					stop = False

				if len(task.params) == 0:
					res = self.ask("No parameters are set. Is this ok? (y/n) ")
					if res.strip().lower() not in ["y", "yes"]:
						stop = False

				try:
					task.timestamps = {"created": time.time()}
					task.save()
					self.ok("created new task {}".format(task.id))
				except errors.TalusApiError as e:
					print(e.message)

			return

		parser = self._argparser()
		parser.add_argument("--name", "-n")
		parser.add_argument("--tool", "-t")
		parser.add_argument("--limit", "-l", default=1)
		parser.add_argument("--params", "-p", default=None)
		parser.add_argument("--version", "-v", default=None)
		parser.add_argument("--params-file", "-f", default=None)
		parser.add_argument("--vm-max", default="30m")

		args = parser.parse_args(args)

		if args.params is None and args.params_file is None:
			sys.stderr.write("Error, params must be specified with either -p or -f")
			return

		if args.params_file is not None:
			if not os.path.exists(args.params_file):
				sys.stderr.write("ERROR, params file does not exist: '{}'".format(args.params_file))
				return

			with open(args.params_file, "r") as f:
				args.params = f.read()

		params = json.loads(args.params)

		task = self._talus_client.task_create(
			name			= args.name,
			params			= json.loads(args.params),
			tool_id			= args.tool,
			limit			= args.limit,
			version			= args.version,
			vm_max			= args.vm_max
		)

		print("created")
	
	def do_delete(self, args):
		"""Delete an existing task

		task delete <TASK_ID_OR_NAME>
		"""
		args = shlex.split(args)
		self._talus_client.task_delete(args[0])
		print("deleted")
	
	def do_run(self, args):
		"""Run an existing task
		"""
		pass
	
	def do_clone(self, args):
		"""Clone an existing task
		"""
		pass
