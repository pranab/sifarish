#!/usr/bin/python

import sys
from random import randint
import time

implRatingFile = sys.argv[1]
percentExpltRating = int(sys.argv[2])

#read implicit rating file
file = open(implRatingFile, "r")
line = file.readline()
while line:
	items = line.split(',')
	rating = int(items[2])
	
	#explicit rating only for some of those fully consumed items
	if (rating == 100 and randint(0, 100) < percentExpltRating):
		explRating = rating - randint(0, 40)
		timeStamp = long(items[3]) + randint(24, 168) * 60 * 60
		print "%s,%s,%d,%d" %(items[0], items[1], explRating, timeStamp)
	line = file.readline()


file.close
