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

class SlaveCmd(TalusCmdBase):
	"""The Talus slave command processor
	"""

	command_name = "slave"

	def do_list(self, args):
		"""List existing slaves connected to Talus.

		slave list

		"""
		parts = shlex.split(args)
		search = self._search_terms(parts, user_default_filter=False)

		headers = ["id", "hostname", "ip", "max_vms", "running_vms"]
		values = []
		for slave in self._talus_client.slave_iter(**search):
			values.append([
				slave.id,
				slave.hostname,
				slave.ip,
				slave.max_vms,
				slave.running_vms
			])
		print(tabulate(values, headers=headers))
	
	def do_info(self, args):
		"""List information about a slave

		talus slave info ID_OR_HOSTNAME_OR_IP

		"""
		if args.strip() == "":
			raise talus_client.errors.TalusApiError("You must provide a slave ip/hostname/id")

		parts = shlex.split(args)
		leftover = []
		slave_id_or_name = None
		search = self._search_terms(parts, out_leftover=leftover, user_default_filter=False)
		if len(leftover) > 0:
			slave_id_or_name = leftover[0]

		slave = self._resolve_one_model(slave_id_or_name, Slave, search, default_id_search=["hostname", "id", "ip"], sort="hostname")

		if slave is None:
			raise talus_client.errors.TalusApiError("Could not locate slave by id/hostname/ip {!r}".format(slave_id_or_name))

		vm_headers = ["tool", "vnc", "running since", "job", "job idx"]
		vm_vals = []
		for vm in slave.vms:
			vm_vals.append([
				vm["tool"],
				vm["vnc_port"],
				arrow.get(vm["start_time"]).humanize(),
				vm["job"],
				vm["idx"]
			])

		if len(slave.vms) == 0:
			vm_infos = ""
		else:
			vm_infos = "\n\n" + "\n".join("    {}".format(x) for x in tabulate(vm_vals, headers=vm_headers).split("\n"))

		print("""
         ID: {id}
       UUID: {uuid}
   Hostname: {hostname}
    IP Addr: {ip}
   Jobs Run: {jobs_run}
    Max VMs: {max_vms}
Running VMs: {running_vms}{vm_infos}
		""".format(
			id			= slave.id,
			uuid		= slave.uuid,
			hostname	= slave.hostname,
			ip			= slave.ip,
			jobs_run	= slave.total_jobs_run,
			max_vms		= slave.max_vms,
			running_vms	= slave.running_vms,
			vm_infos	= vm_infos
		))
