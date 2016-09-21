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

catColors = {}
catColors["cell phone"] =["white", "black", "gold", "silver"]
catColors["tablet"] =["white", "black", "silver"]
catColors["laptop"] =["black", "silver"]
catColors["wearables"] =["white", "black", "gold", "silver", "blue"]

catPrice = {"cell phone" : (150,80), "tablet" : (200,100), "laptop" : (500,200), "wearables" : (100,50)}

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

def createItems(itemCount):
	for i in range(0,itemCount):
		allItems.append(genID(10))
		
def itemCatBrand(allItems, categories, catBrands):
	for it in allItems:
		cat = selectRandomFromList(categories)
		brands = catBrands[cat]
		if (randint(0,9) < 4):
			brand = brands[0]
		else:
			brand = selectRandomFromList(brands) 
		color = selectRandomFromList(catColors[cat]) 
		price = catPrice[cat][0] +  randint(0,catPrice[cat][1])
		
		print "%s,%s,%s,%s,%d" %(it, cat, brand, color, price)
		
##########################################################################
op = sys.argv[1]

if (op == "existingItems"):	 
	eventFile = sys.argv[2]
	loadItems(eventFile)
	
elif (op == "newItems"):	
	itemCount = int(sys.argv[2])
	createItems(itemCount)

	
itemCatBrand(allItems, categories, catBrands)

