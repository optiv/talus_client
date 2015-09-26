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

class CorpusCmd(TalusCmdBase):
	"""The Talus job command processor
	"""

	command_name = "corpus"

	def do_delete(self, args):
		"""Delete the file specified by the provided id
		"""
		parts = shlex.split(args)
		if len(parts) == 0:
			raise errors.TalusApiError("You must provide the id of the file to delete")

		file_id = parts[0]
		res = self._talus_client.corpus_delete(file_id)
		if "error" in res:
			raise errors.TalusApiError("Could not delete file id {!r}: {}".format(
				file_id,
				res["error"],
			))

		print("deleted")

	def do_get(self, args):
		"""Get the file(s) from the corpus with the provided id(s), saving to the
		current directory if no destination path is provided (works like cp and mv)

		Example:

		To fetch a single file and save it into /tmp:

			corpus get 55cdcbaedd18da0008caa793 /tmp

		To fetch multiple files and save to /tmp:

			corpus get 55cdcbaedd18da0008caa793 55cdcbaedd18da0008caa794 55cdcbaedd18da0008caa795 /tmp
		"""
		parts = shlex.split(args)

		if len(parts) == 0:
			raise errors.TalusApiError("At least one id must be provided")

		if len(parts) > 1:
			dest = parts[-1]
			file_ids = parts[:-1]
		else:
			dest = None
			file_ids = [parts[0]]

		if dest is None:
			dest = "./"

		full_dest = os.path.abspath(os.path.expanduser(dest))
		
		# it needs to be a directory for this to work
		if len(file_ids) > 1 and (not os.path.exists(full_dest) or not os.path.isdir(full_dest)):
			raise errors.TalusApiError("Destination for multiple files must exist _and_ be a directory!")

		for file_id in file_ids:
			fname,data = self._talus_client.corpus_get(file_id)

			if len(file_ids) > 1 or os.path.isdir(full_dest):
				unexpanded_dest = os.path.join(dest, fname)
				write_dest = os.path.join(full_dest, fname)
			else:
				unexpanded_dest = dest
				write_dest = full_dest

			with open(write_dest, "wb") as f:
				f.write(data)

			print("{} saved to {} ({} bytes)".format(
				file_id,
				unexpanded_dest,
				len(data)
			))

	def do_list(self, args):
		"""List all of the corpus files in talus. All supplied arguments will be used
		to filter the results

		corpus list [--SEARCH_KEY SEARCH_VAL ...] [-l]

			-l,--list    List only the ids (defaults to more info)
		
		Examples:
		
		To list all files with md5 ABCD:
		
			corpus list --md5 ABCD

		To list all files with size 1337:

			corpus list --length 1337

		To list all files with extra attribute urmom equal to blah:

			corpus list --urmom blah

		To list all files of content-type "text/plain":
			
			corpus list --contentType text/plain
		"""
		parts = shlex.split(args)
		filters = {}

		ids_only = False

		while len(parts) != 0:
			key = parts[0]
			if key == "-l" or key == "--list":
				ids_only = True
				parts = parts[1:]
				continue

			if key.startswith("-"):
				key = key.replace("-", "")
				if key not in ["md5", "id", "_id", "length", "content_type", "contentType"] and not key.startswith("metadata"):
					key = "metadata." + key

				value = parts[1]

				if re.match(r'^[0-9]+$', value):
					value = int(value)

				# allow multiple values - gets treated as looking for that key's value
				# to be one of the provided values. E.g. id in ["ID1", "ID2", "ID3", ...]
				if key in filters:
					if not isinstance(filters[key], list):
						filters[key] = [filters[key]]
					filters[key].append(value)
				else:
					filters[key] = value

				parts = parts[2:]
			else:
				parts = parts[1:]

		res = self._talus_client.corpus_list(**filters)

		if ids_only:
			for cfile in res:
				print(cfile["_id"]["$oid"])
			return

		headers = ["id", "size (bytes)", "md5", "content-type", "upload date", "other attrs"]
		values = []

		print("{} corpus files found".format(len(res)))
		for cfile in res:
			# {
			#	u'contentType': u'text/plain',
			#	u'chunkSize': 261120,
			#	u'metadata': {u'filename': None},
			#	u'length': 5,
			#	u'uploadDate': {u'$date': 1439550357245},
			#	u'_id': {u'$oid': u'55cdcb95dd18da0008caa791'},
			#	u'md5': u'0d599f0ec05c3bda8c3b8a68c32a1b47'
			#}
			values.append([
				cfile["_id"]["$oid"],
				cfile["length"],
				cfile["md5"],
				cfile["contentType"],
				arrow.get(cfile["uploadDate"]["$date"]/1000.0).humanize(),
				" ".join("{}={}".format(k,v) for k,v in cfile["metadata"].iteritems())
			])

		print(tabulate(values, headers=headers))
	
	def do_upload(self, args):
		"""Upload a file into the talus corpus

		corpus upload FILE_PATHS [--attr1 value1 [--attr2 value2] ...]

		Examples:

		To upload all files named "*.swf" with extra attribute tag being set to
		"cool games", do:

			corpus upload *.swf --tag "cool games"
		"""
		parts = shlex.split(args)

		files = []
		extra_attrs = {}
		while len(parts) != 0:
			key = parts[0]
			if key.startswith("-"):
				key = key.replace("-", "")
				value = parts[1]
				extra_attrs[key] = value
				parts = parts[2:]
			else:
				files += glob.glob(parts[0])

				# it's a file name/glob expression
				parts = parts[1:]

		uploaded_ids = []
		for file_path in files:
			file_path = os.path.abspath(file_path)
			corpus_id = self._talus_client.corpus_upload(file_path, **extra_attrs)
			uploaded_ids.append(corpus_id)
			print("{} - {}".format(corpus_id, file_path))
