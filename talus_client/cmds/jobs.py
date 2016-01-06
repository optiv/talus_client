#!/usr/bin/env python
# encoding: utf-8

import argparse
import arrow
import cmd
import datetime
import json
import math
import os
import re
import shlex
import sys
from tabulate import tabulate
import textwrap
import time

from talus_client.cmds import TalusCmdBase
import talus_client.api
import talus_client.errors as errors
import talus_client.utils as utils
from talus_client.models import *
from talus_client.param_model import ModelCmd

class JobCmd(TalusCmdBase):
	"""The Talus job command processor
	"""

	command_name = "job"

	def do_info(self, args):
		"""List detailed information about the job.

		Git-like syntax can also be used here to refer to the most recently created
		job. E.g. the command below will show info about the 2nd most recent job:

			job info +2

		Search information can also be used. If git-like syntax is omitted, only
		the first entry returned from the database will be displayed.

			job info --all --status running --sort -priority +2

		The example above will show information about 2nd highest priority job whose
		status is running. Omitting --all will cause the search to be performed only
		among _your_ jobs.
		"""
		if args.strip() == "":
			raise errors.TalusApiError("you must provide a name/id of a job to show info about it")

		parts = shlex.split(args)

		all_mine = False
		# this is the default, so just remove this flag and ignore it
		if "--all-mine" in parts:
			parts.remove("--all-mine")

		leftover = []
		job_id_or_name = None
		search = self._search_terms(parts, out_leftover=leftover)

		if len(leftover) > 0:
			job_id_or_name = leftover[0]

		job = self._resolve_one_model(job_id_or_name, Job, search)

		if job is None:
			raise errors.TalusApiError("could not find talus job with id {!r}".format(job_id_or_name))

		job_errors = "None"
		job_logs = "None"
		# created value
		cv = None
		created = ""
		if "created" in job.timestamps:
			cv = job.timestamps["created"]
			created = "{} ({})".format(self._rel_date(cv), self._actual_date(cv))

		stopped = ""
		# stopped value
		sv = None
		if job.status["name"] == "cancelled" and "cancelled" in job.timestamps:
			sv = job.timestamps["cancelled"]
		elif job.status["name"] == "finished" and "finished" in job.timestamps:
			sv = job.timestamps["finished"]

		if sv is not None:
			stopped = "{} ({})".format(self._rel_date(sv), self._actual_date(sv))

		# in limits/s
		if sv is None:
			sv = time.time()

		speed = ""

		if cv is not None:
			# in limits/sec
			speed_val = job.progress / (sv - cv)

			limits_sec = round(speed_val, 2)
			limits_min = round(speed_val * 60, 2)
			limits_hour = round(speed_val * 60*60, 2)
			limits_day = round(speed_val * 60*60*24, 2)
			speed = "{}/s, {}/min, {}/hour, {}/day".format(
				limits_sec,
				limits_min,
				limits_hour,
				limits_day
			)

		if len(job.errors) > 0:
			job_errors = "\n\n"
			for idx,error in enumerate(job.errors):
				job_errors += "\n".join([
					"ERROR {}".format(idx),
					"---------",
					"\n".join("    {}".format(x) for x in error["logs"]),
					error["message"],
					error["backtrace"],
					"\n\n",
				])

			job_errors = "\n".join(["    {}".format(x) for x in job_errors.split("\n")])

		if len(job.logs) > 0:
			job_logs = "\n\n"
			for idx,log in enumerate(job.logs):
				job_logs += "\n".join([
					"LOG {}".format(idx),
					"---------",
					log["message"],
					"\n".join("    {}".format(x) for x in log["logs"]),
					"\n\n",
				])

			job_logs = "\n".join(["    {}".format(x) for x in job_logs.split("\n")])

		print("""
         ID: {id}
       Name: {name}
     Status: {status}
       Tags: {tags}
    Started: {started}
      Ended: {ended}
      Debug: {debug}
      Speed: {speed}
   Progress: {percent}% ({progress} / {limit})
       Task: {task}
   Priority: {priority}
     Params: {params}
    Network: {network}
      Image: {image}
     VM Max: {vm_max}
Running VMS: {running_vms}
     Errors: {job_errors}
       Logs: {job_logs}
		""".format(
			id			= job.id,
			name		= job.name,
			status		= job.status["name"],
			tags		= job.tags,
			started		= created,
			ended		= stopped,
			debug		= job.debug,
			speed		= speed,
			percent		= round(100 * job.progress / float(job.limit), 1),
			progress	= job.progress,
			limit		= job.limit,
			task		= self._nice_name(job, "task"),
			priority	= job.priority,
			params		= json.dumps(job.params),
			network		= job.network,
			image		= self._nice_name(job, "image"),
			vm_max		= job.vm_max,
			running_vms	= self._get_running_vms(job),
			job_errors	= job_errors,
			job_logs	= job_logs,
		))
	
	def _get_running_vms(self, job):
		vm_headers = ["slave", "vnc port", "running since", "job idx", "status"]
		vm_vals = []

		for slave in self._talus_client.slave_iter():
			for vm in slave.vms:
				if vm["job"] == job.id:
					vm_vals.append([
						slave.hostname,
						vm["vnc_port"],
						arrow.get(vm["start_time"]).humanize(),
						vm["idx"],
						vm["vm_status"]
					])

		if len(vm_vals) == 0:
			return "None"

		split = int(math.ceil(len(vm_vals)/2.0))
		column1 = vm_vals[:split]
		column2 = vm_vals[split:]

		table1 = tabulate(column1, headers=vm_headers).split("\n")
		table2 = tabulate(column2, headers=vm_headers).split("\n")

		longest_t1 = max(len(x) for x in table1)

		if len(table2) == 0:
			lines = table1
		else:
			lines = []
			for x in xrange(len(table1)):
				if x >= len(table2):
					lines.append(table1[x])
				else:
					fmt_string = "{:" + str(longest_t1) + "}  |  {}"
					lines.append(fmt_string.format(table1[x], table2[x]))

		return "\n\n" + "\n".join(lines) + "\n"

	def do_list(self, args):
		"""List jobs in Talus.

			job list --search-term value --search-term2 value

		By default only running jobs that belong to you are shown. To show
		all of your jobs, add --all-mine:

			job list --search-term value --all-mine

		To show all jobs, use `--all`:

			job list --all

		Dot notation can be used on subdocuments. The example below is
		the verbose form of `--status cancelled`:

			job list --status.name cancelled

		Sorting, skipping, and number of results can also be set using
		`--sort field`, `--skip num`, and `--num num` respectively. A sort
		value preceded by a negative sign reverses the sorting order:

			job list --status finished --sort -timestamps.finished --skip 10 --num 5

		MongoDB operators are allowed (don't forget to escape the $). See
		https://docs.mongodb.org/manual/reference/operator/query/:
			
			job list --limit.\\$gt 10
			job list --name.\\$regex ".*test.*"
			job list --$where "(this.progress / this.limit) > 0.5"

		MongoEngine operators are allowed as well. See
		http://docs.mongoengine.org/guide/querying.html#query-operators:

			job list --name__startswith "test"
			job list --limit__gt 10
		"""
		parts = shlex.split(args)

		all_mine = False
		if "--all-mine" in parts:
			parts.remove("--all-mine")
			all_mine = True

		search = self._search_terms(parts)
		if not all_mine and "status.name" not in search and "--all" not in parts:
			self.out("use --all-mine to view all of your jobs (not just unfinished)")
			search["status.name"] = "running"

		if not all_mine and "--all" not in parts and "num" not in search:
			self.out("showing first 20 results")
			search["num"] = 20

		if "sort" not in search:
			search["sort"] = "-timestamps.created"

		headers = ["id", "name", "status", "priority", "progress", "image", "task", "tags"]
		fields = []
		for job in self._talus_client.job_iter(**search):
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
				self._nice_name(job, "image"),
				self._nice_name(job, "task"),
				job.tags
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
		if args.strip() == "":
			raise errors.TalusApiError("you must provide a name/id of a job to show info about it")

		parts = shlex.split(args)
		leftover = []
		job_id_or_name = None
		search = self._search_terms(parts, out_leftover=leftover)
		if len(leftover) > 0:
			job_id_or_name = leftover[0]

		job = self._resolve_one_model(job_id_or_name, Job, search)
		if job is None:
			raise errors.TalusApiError("no jobs matched id/search criteria")

		self._talus_client.job_cancel(job_id_or_name, job=job)

		self.ok("cancelled job {}".format(job.id))
	
	def do_clone(self, args):
		"""Create a new job that is an exact duplicate of a previously created
		job. Note that +1 and other search parameters can be used to identify the
		job to clone.
		"""
		if args.strip() == "":
			raise errors.TalusApiError("you must provide a name/id of a job to show info about it")

		parts = shlex.split(args)
		leftover = []
		job_id_or_name = None
		search = self._search_terms(parts, out_leftover=leftover)
		if len(leftover) > 0:
			job_id_or_name = leftover[0]

		job = self._resolve_one_model(job_id_or_name, Job, search)

		if job is None:
			raise errors.TalusApiError("could not find talus job with id {!r}".format(job_id_or_name))

		old_id = job.id
		job.clear_id()
		clone_match = re.match(r'^(.*)_CLONE_(\d+)$', job.name)
		if clone_match is not None:
			job.name = "{}_CLONE_{}".format(clone_match.group(1), int(clone_match.group(2))+1)
		else:
			job.name += "_CLONE_0"
		job.timestamps = {"created": time.time()}
		job.status = {"name": "run"}
		job.errors = []
		job.logs = []
		job.progress = 0
		self._prep_model(job) # make sure our username is tagged in it

		if self._go_interactive(parts):
			self.do_create("--shell", job=job)
		else:
			job.save()

		self.ok("created job {} ({}) as clone of {}".format(job.name, job.id, old_id))
	
	def do_create(self, args, job=None):
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
			   -t,--tags	A comma-separated list of additional tags to add to the job
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
			if job is None:
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
				job.tags = task.tags
				self._prep_model(job)
				job.image = task.image
				job.task = task.id
				job.limit = task.limit
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
		parser.add_argument("--tags", "-t", default="")
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

		tags = args.tags.split(",")

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
			debug			= args.debug,
			tags			= tags
		)

		self.ok("created job {}".format(job.id))
