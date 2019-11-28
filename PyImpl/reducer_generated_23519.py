#!/usr/bin/python3
import csv
import sys

countcol0 = 0

for line in sys.stdin:
	try:
		if (len(line.strip()) > 0):
			values1 = line.split(',')
			values = [x.strip() for x in values1]
			countcol0 += 1
			

	except:
		pass
print("count: ", countcol0)
