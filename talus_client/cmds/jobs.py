#!/usr/bin/env python
# encoding: utf-8

import argparse
import cmd
import datetime
import json
import os
import re
import shlex
import sys
from tabulate import tabulate
import textwrap
import time

from talus_client.cmds import TalusCmdBase
import talus_client.api
import talus_client.errors
import talus_client.utils as utils
from talus_client.models import *
from talus_client.param_model import ModelCmd

class JobCmd(TalusCmdBase):
	"""The Talus job command processor
	"""

	command_name = "job"

	def do_info(self, args):
		"""List detailed information about the job. Probably used mainly for viewing
		errors from a job
		"""
		job = shlex.split(args)[0]
		search = {}
		job = self._talus_client.job_find(job, **search)

		if job is None:
			raise errors.TalusApiError("could not find talus job with id {!r}".format(job))

		errors = "None"

		if len(job.errors) > 0:
			errors = "\n\n"
			for idx,error in enumerate(job.errors):
				errors += "\n".join([
					"ERROR {}".format(idx),
					"---------",
					error["message"],
					"\n".join("    {}".format(x) for x in error["logs"]),
					"\n\n",
				])

			errors = "\n".join(["    {}".format(x) for x in errors.split("\n")])

		print("""
      ID: {id}
    Name: {name}
   Debug: {debug}
  Status: {status}
Progress: {progress}
    Task: {task}
Priority: {priority}
  Params: {params}
 Network: {network}
   Image: {image}
  VM Max: {vm_max}
  Errors: {errors}
		""".format(
			id			= job.id,
			name		= job.name,
			debug		= job.debug,
			status		= job.status["name"],
			progress	= job.progress,
			task		= job.task,
			priority	= job.priority,
			params		= json.dumps(job.params),
			network		= job.network,
			image		= job.image,
			vm_max		= job.vm_max,
			errors		= errors
		))

	def do_list(self, args):
		"""List existing jobs in Talus.

		job list

		"""
		headers = ["id", "name", "status", "priority", "progress", "image"]
		fields = []
		for job in self._talus_client.job_iter():
			status = job.status["name"]
			if len(job.errors) > 0:
				status += " *E"

			fields.append([
				str(job.id),
				job.name,
				status,
				job.priority,
				"{:0.2f}% ({}/{})".format(
					job.progress / float(job.limit) * 100,
					job.progress,
					job.limit
				),
				job._fields["image"]["name"]
			])
		print(tabulate(fields, headers=headers))

	def complete_cancel(self, text, line, bg_idx, end_idx):
		"""Do completion for the cancel command
		"""
		total = []
		for job in self._talus_client.job_iter():
			total.append(job.name)
			total.append(job.id)
		matching = filter(lambda x: x.startswith(text), total)
		return matching
	
	def do_cancel(self, args):
		"""Cancel the job by name or ID in talus

		job cancel JOB_NAME_OR_ID

		"""
		job_name_or_id = shlex.split(args)[0]

		job = self._talus_client.job_cancel(job_name_or_id)

		print("stopped")
	
	def do_create(self, args):
		"""Create a new job in Talus

		job create TASK_NAME_OR_ID -i IMAGE [-n NAME] [-p PARAMS] [-q QUEUE] [--priority (0-100)] [--network]

		       -n,--name    The name of the job (defaults to name of the task + timestamp)
		      --priority    The priority for the job (0-100, defaults to 50)
			   --network    The network for the image ('all' or 'whitelist'). Whitelist values may
			                also be a 'whitelist:<domain_or_ip>,<domain_or_ip>' to add domains
							to the whitelist. Not specifying additional whitelist hosts results
							in a host-only network filter, plus talus-essential hosts.
			  -q,--queue    The queue the job should be inserted into (default: jobs)
			  -i,--image    The image the job should run in (name or id)
		      -l,--limit    The limit for the task. What the limit means is defined by how the tool
			                reports progress. If the tool does not report progress, then the limit
			                means the number of total VMs to run.
				--vm-max    Maximum amount of time a vm should be allowed to run (defaults to 30m)
				            You may use values such as 30m15s. If no units are used, the value is
							assumed to be in seconds.
		     -p,--params    Params for the task (defaults to the default params of the task)
			     --shell    Create the job in an interactive shell (default if already in shell and no args)
				 --debug	All logs are saved to the database (treated as errored, basically)
		-f,--params-file    The file that contains the params of the job

		Examples:

		To run the task "CalcFuzzer" while only updating the ``chars`` parameter:

		    job create "CalcFuzzer" -p '{"chars": "013579+-()/*"}'
		"""
		args = shlex.split(args)
		if self._go_interactive(args):
			tasks = list(self._talus_client.task_iter())
			fields = []
			for x in xrange(len(tasks)):
				task = tasks[x]
				fields.append([x, task.name, task.id])

			headers = ["idx", "name", "task.id"]

			idx = utils.idx_prompt(fields, "Which task should the job be based on?", headers=headers)
			if idx is None:
				return

			task = tasks[idx]
			job = Job(api_base=self._talus_client._api_base)
			self._prep_model(job)
			job.image = task.image
			job.task = task.id
			job.name = task.name + " " + str(datetime.datetime.now())
			job.params = task.params
			job.status = {"name": "run"}
			job.vm_max = task.vm_max
			job.queue = "jobs"

			self.out("basing job on task named {!r} ({})".format(task.name, task.id))

			while True:
				param_cmd = self._make_model_cmd(job)
				cancelled = param_cmd.cmdloop()
				if cancelled:
					break

				error = False
				if job.name is None:
					self.err("Please set a name for the job")
					error = True

				if job.image is None:
					self.err("You need to set an image, yo")
					error = True

				if error:
					continue

				try:
					job.timestamps = {"created": time.time()}
					job.save()
					self.ok("created new job {}".format(job.id))
				except errors.TalusApiError as e:
					self.err(e.message)
				else:
					break

			return

		parser = self._argparser()
		parser.add_argument("task_name_or_id")
		parser.add_argument("--name", "-n", default=None)
		parser.add_argument("--network", default="whitelist")
		parser.add_argument("--priority", default=50)
		parser.add_argument("--limit", "-l", default=None)
		parser.add_argument("--image", "-i", default=None)
		parser.add_argument("--queue", "-q", default="jobs")
		parser.add_argument("--params", "-p", default=None)
		parser.add_argument("--params-file", "-f", default=None)
		parser.add_argument("--vm-max", default="30m")
		parser.add_argument("--debug", default=False, action="store_true")

		args = parser.parse_args(args)

		params = args.params
		if args.params_file is not None:
			if not os.path.exists(args.params_file):
				raise errors.TalusApiError("params file does not exist: {}".format(args.params_file))
			with open(args.params_file, "r") as f:
				params = f.read()

		if params is not None:
			try:
				params = json.loads(params)
			except Exception as e:
				raise errors.TalusApiError("params are not in json format: " + e.message)

		job = self._talus_client.job_create(
			task_name_or_id	= args.task_name_or_id,
			name			= args.name,
			image			= args.image,
			params			= params,
			priority		= args.priority,
			limit			= args.limit,
			queue			= args.queue,
			network			= args.network,
			vm_max			= args.vm_max,
			debug			= args.debug
		)

		self.ok("created job {}".format(job.id))
