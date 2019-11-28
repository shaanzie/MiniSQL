#!/usr/bin/python3
import csv
import sys

for line in sys.stdin:
	values1 = line.lower().split(',')
	values = [x.strip() for x in values1]
	try:
		print(line)
	except:
		pass
