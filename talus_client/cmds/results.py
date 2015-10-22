#!/usr/bin/env python
# encoding: utf-8

import argparse
import cmd
import os
import shlex
import sys
from tabulate import tabulate
import textwrap

from talus_client.cmds import TalusCmdBase
import talus_client.api
import talus_client.errors as errors
from talus_client.models import *

class ResultCmd(TalusCmdBase):
	"""The talus result command processor
	"""

	command_name = "result"

	def do_list(self, args):
		"""List results in talus for a specific job. Fields to be searched for must
		be turned into parameter format (E.g. ``--search-item "some value"`` format would search
		for a result with the field ``search_item`` equaling ``some value``).

		result list --search-item "search value" [--search-item2 "search value2" ...]
		"""
		parts = shlex.split(args)

		all_mine = False
		if "--all-mine" in parts:
			parts.remove("--all-mine")
			all_mine = True

		search = self._search_terms(parts)

		if "sort" not in search:
			search["sort"] = "-created"

		if "--all" not in parts and not all_mine and "num" not in search:
			search["num"] = 20
			self.out("showing first 20 results")

		print(tabulate(self._talus_client.result_iter(**search), headers=Result.headers()))
