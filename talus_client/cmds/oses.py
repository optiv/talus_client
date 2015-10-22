#!/usr/bin/env python
# encoding: utf-8

import argparse
import cmd
import os
import shlex
import sys
from tabulate import tabulate

from talus_client.cmds import TalusCmdBase
import talus_client.api
import talus_client.errors as errors
from talus_client.models import *

class OsCmd(TalusCmdBase):
	"""The Talus code command processor
	"""

	command_name = "os"
	
	def do_list(self, args):
		"""List all operating system models defined in Talus
		"""
		parts = shlex.split(args)

		search = self._search_terms(parts)

		if "sort" not in search:
			search["sort"] = "name"

		print(tabulate(self._talus_client.os_iter(**search), headers=OS.headers()))

	def do_create(self, args):
		"""Create a new operating system model in Talus

		create -n NAME [--type TYPE] [-t TAG1,TAG2,..] [-v VERSION]

		   -n,--name    The name of the new OS model (required, no default)
		   -t,--type    The type of the OS mdoel (default: "windows")
		   -a,--arch	The architecture of the OS (default: "x64")
		-v,--version    The version of the new OS model (default: "")

		Examples:

		To create a new operating system model for an x64 Windows 7 OS:

		    os create -n "Windows 7 x64" -t windows -v 7 -a x64
		"""
		args = shlex.split(args)
		if self._go_interactive(args):
			os = OS()
			self._prep_model(os)
			os.version = ""
			os.arch = "x64"
			while True:
				model_cmd = self._make_model_cmd(os)
				cancelled = model_cmd.cmdloop()
				if cancelled:
					break

				error = False
				if os.name is None or os.name.strip() == "":
					self.err("You must give the OS a name")
					error = True

				if os.type is None:
					self.err("You must specify an os type (linux/windows)")
					error = True
				elif os.type not in ["linux", "windows"]:
					self.err("Sorry man, os.type must be one of 'linux' or 'windows'")
					error = True

				if error:
					continue

				try:
					os.save()
					self.ok("created new os {}".format(os.id))
				except errors.TalusApiError as e:
					self.err(str(e))
				else:
					break
			return

		parser = self._argparser()
		parser.add_argument("--name", "-n")
		parser.add_argument("--type", "-t", default="windows")
		parser.add_argument("--version", "-v", default="")
		parser.add_argument("--arch", "-a", default="x64")

		args = parser.parse_args(args)

		new_os = OS(self._talus_host)
		new_os.name = args.name
		new_os.type = args.type
		new_os.version = args.version
		new_os.arch = args.arch

		try:
			new_os.save()
			print("created")
		except talus_client.errors.TalusApiError as e:
			sys.stderr.write("Error saving OS: {}\n".format(e.message))
	
	def do_delete(self, args):
		"""Delete an operating system model in Talus

		os delete <OS_ID_OR_NAME>
		"""
		args = shlex.split(args)
		self._talus_client.os_delete(args[0])
		print("deleted")
