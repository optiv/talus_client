#!/usr/bin/env python
# encoding: utf-8

import argparse
import arrow
import cmd
import glob
import json
import os
import shlex
import sys
from tabulate import tabulate
import textwrap

from talus_client.cmds import TalusCmdBase
import talus_client.api
import talus_client.errors
from talus_client.models import *

class FileSetCmd(TalusCmdBase):
	"""The Talus job command processor
	"""

	command_name = "fileset"

	def do_info(self, args):
		"""List information about a fileset given its name or id

		fileset info NAME_OR_ID

		-a,--all   List all files associated with the fileset
		"""
		parser = self._argparser()
		parser.add_argument("name_or_id")
		parser.add_argument("--all", "-a", default=False, action="store_true")

		args = parser.parse_args(shlex.split(args))

		if args.name_or_id == "":
			raise errors.TalusApiError("You must provide the id or name of the fileset to get info about")

		search = {}
		fileset = self._talus_client.fileset_find(args.name_or_id, **search)
		if fileset is None:
			raise errors.TalusApiError("Fileset could not be located by name/id {!r}".format(fileset))

		if args.all:
			files_str = ""
		else:
			files_str = "{} files".format(len(fileset.files))

		ts = {"created":"", "modified":""}
		if "created" in fileset.timestamps:
			ts["created"] = arrow.get(fileset.timestamps["created"]).humanize()
		if "modified" in fileset.timestamps:
			ts["modified"] = arrow.get(fileset.timestamps["modified"]).humanize()

		print("""
      ID: {id}
    Name: {name}
     Job: {job}
 Created: {created}
Modified: {modified}
   Files: {files}
		""".format(
			id			= fileset.id,
			name		= fileset.name,
			job			= fileset.job if fileset.job is not None else "",
			created		= ts["created"],
			modified	= ts["modified"],
			files		= files_str
		))

		if args.all:
			arg_str = " ".join("--id {}".format(x) for x in fileset.files)

			# _root is set in cmds/__init__.py:TalusCmd._add_command
			self._root.onecmd("corpus list {}".format(arg_str))

	def do_get(self, args):
		"""Fetch the entire fileset
		"""
		pass
	
	def do_list(self, args):
		"""List defined filesets
		"""
		headers = ["id", "name", "num files", "job", "created", "modified"]
		fields = []
		for fileset in self._talus_client.fileset_iter():
			ts = {"modified" : "", "created": ""}
			if "modified" in fileset.timestamps:
				ts["modified"] = arrow.get(fileset.timestamps["modified"]).humanize()
			if "created" in fileset.timestamps:
				ts["created"] = arrow.get(fileset.timestamps["created"]).humanize()

			fields.append([
				str(fileset.id),
				fileset.name,
				len(fileset.files),
				fileset.job,
				ts["created"],
				ts["modified"]
			])

		print(tabulate(fields, headers=headers))
	
	def do_create(self, args):
		"""Create a new fileset in Talus

		fileset create -n FILESET_NAME [FILE_ID1 [FILE_ID2 ..]]

		-f,--from-files    Create the fileset from a list of files instead of corpus ids
		-n,--name          The name of the fileset

		Example:

		Create a fileset named "test fileset" with the files ABC and DEF:

			fileset create -n "test fileset" ABC DEF
		"""
		parser = self._argparser()
		parser.add_argument("files", nargs="*")
		parser.add_argument("--from-files", "-f", action="store_true", default=False)
		parser.add_argument("--name", "-n", default=None)

		args = parser.parse_args(shlex.split(args))

		if args.name is None:
			raise errors.TalusApiError("You must provide a name for the file set")

		if args.from_files:
			fileids = []
			for filename in args.files:
				if not os.path.exists(filename):
					print("file {} not found".format(filename))
					continue

				file_path = os.path.abspath(filename)
				fileid = self._talus_client.corpus_upload(filename)
				fileids.append(str(fileid))
		else:
			fileids = args.files

		res = self._talus_client.fileset_create(
			name	= args.name,
			files	= fileids
		)

		print("created")
	
	def do_delete(self, args):
		"""Delete an existing fileset by name or id. Optionally delete all files within
		the fileset as well (TODO)

		fileset delete NAME_OR_ID
		"""
		name_or_id = shlex.split(args)[0]

		self._talus_client.fileset_delete(name_or_id)
		print("deleted")
