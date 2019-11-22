import csv
import sys
for line in sys.stdin:
	values = line.split(',')
	if values[0] >3.4 :
		print(line)
