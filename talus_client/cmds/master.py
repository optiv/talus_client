#!/usr/bin/env python
# encoding: utf-8

import argparse
import arrow
import cmd
import os
import shlex
import sys
from tabulate import tabulate
import textwrap

from talus_client.cmds import TalusCmdBase
import talus_client.api
import talus_client.errors
from talus_client.models import *

class MasterCmd(TalusCmdBase):
	"""The Talus master command processor
	"""

	command_name = "master"

	def do_info(self, args):
		"""Show info about the Talus master
		"""
		master = self._talus_client.master_get()

		indent = " "*10

		vms = []
		if len(master.vms) > 0:
			vms.append(master.vms[0]["uri"])
			for vm in master.vms[1:]:
				vms.append(indent + vm["uri"])
		vms = "\n".join(vms)
		if vms.strip() == "":
			vms = "None"

		queues = []
		num_jobs = 0
		headers = ["priority", "job name", "job id", "prog/limit", "tags"]
		for qname,jobs in master.queues.iteritems():
			queues.append(indent + qname)
			fields = []
			for job_info in jobs:
				job = self._talus_client.job_find(job_info["job"])
				num_jobs += 1
				fields.append([
					job_info["priority"],
					job_info["job_name"],
					job_info["job"],
					"{} / {}".format(job.progress, job.limit),
					job.tags,
				])
			table = tabulate(fields, headers=headers)
			queues.append("\n".join(indent + "    " + x for x in table.split("\n")))

		queues = "\n" + "\n".join(queues)
		if num_jobs == 0:
			queues = "None"

		print("""
hostname: {hostname}
      ip: {ip}
     vms: {vms}
  queues: {queues}
		""".format(
			hostname	= master.hostname,
			ip			= master.ip,
			vms			= vms,
			queues		= queues
		))
