#!/usr/bin/python

import os
import sys
from random import randint
import time
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *


#key : user value: list of items
catBrands = {}

allItems = []
categories = ["cell phone", "tablet", "laptop", "wearables"]
catBrands["cell phone"] =["apple", "samsung", "Nokia", "LG", "HTC"]
catBrands["tablet"] = ["samsung", "apple", "amazon kindle", "google chromo"] 
catBrands["laptop"] = ["lenovo", "hp", "acer", "asus", "thinkpad"]
catBrands["wearables"] = ["fitbit", "garmin", "jawbone", "misfit"]


# load user, item and event file
def loadItems(eventFile):
	itemSet = set()
	file = open(eventFile, 'r')

	#read file
	for line in file:
		line.strip()
		tokens = line.split(',')	
		item = tokens[2]
		itemSet.add(item)
		
	file.close()

	#generate items list
	for it in itemSet:
		allItems.append(it)
		
	#print "loaded event history to find unique items count %d" %(len(allItems))
	
def itemCatBrand(allItems, categories, catBrands):
	for it in allItems:
		cat = selectRandomFromList(categories)
		brands = catBrands[cat]
		if (randint(0,9) < 4):
			brand = brands[0]
		else:
			brand = selectRandomFromList(brands)
			
		print "%s,%s,%s" %(it, cat, brand)
		
##########################################################################
eventFile = sys.argv[1]
loadItems(eventFile)
itemCatBrand(allItems, categories, catBrands)

