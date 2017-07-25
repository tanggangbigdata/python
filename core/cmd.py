#!/usr/bin/env python
# _*_ coding:utf-8 _*_
__create_time__="2017-03-09"
__Author__ = "Tanggang"
__note__ = ""

import commands

class cmd:
	def run(self,cmd):
		return commands.getoutput(cmd)

