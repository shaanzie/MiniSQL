import csv
import sys
for line in sys.stdin:
	values = line.split(',')
	print(values[0], values[1], values[2], sep = ' ')
