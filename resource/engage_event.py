#!/usr/bin/python

import sys
from random import randint
import thread
import time
import redis
import uuid

numSession = 10
eventCountMin = 20
eventCountMax = 40

#key : user value: list of items
userItems = {}

allItems = []
lowEngaeEvents = [2,3,4]
midEngageEvents = [1]
terminalEvent = 0

# load user, item and event file
def loadUsersAndItems(eventFile):
	itemSet = set()
	file = open(eventFile, 'r')

	#read file
	for line in file:
		line.rstrip()
    	tokens = line.split(',')	
		items = userItems[tokens[0]]
		if items is None:
			items = [tokens[1]]
			userItems[tokens[0]] = items
		else:
			items.add(tokens[1])
		
		itemSet.add(tokens[1])
		
	file.close()

	#generate items list
	for it in itemSet:
		allItems.add(it)
		
	print "loaded event history with %d users and %d items" %(len(userItems.keys()), len(allItems))
	
# generates (userID, itemID, sessionID, event, time) tuples	
def sessionSimulate(threadName, maxEvent):
	print "starting %s with max event %d" %(threadName, maxEvent)
	engagedItems = {}
	
	#choose user
	users = userItems.keys()
	user = selectRandomFromList(users)
	
	#sessionID
	session = uuid.uuid1()
	
	evCount = 0
	done = False
	while (evCount < maxEvent and not Done):
		if (randin(0,9) < 7 and  len(engagedItems) > 2):
			#choose item already engaged with in this session
			item = selectRandomFromList(engagedItems.keys())
		else:
			if (randin(0,9) < 7):
				#choose an item from past history
				item = selectRandomFromList(userItems[user])
			else:
				#choose something new
				item = selectRandomFromList(allItems)
				
		#choose event type
		events = engagedItems[item]
		if (events is None):
			#not engaged before
			event = selectRandomFromList(lowEngaeEvents)
			events = set()
			engagedItems[item] = events
		else:
			#engaged before
			if 0 in events:
				done = True
			elif 1 in events:
				if (randin(0,9) < 7):
					event = 0;
				else:
					event = selectRandomFromList(lowEngaeEvents)
			else:
				event = selectRandomFromList(lowEngaeEvents)

		events.add(event)				
		evCount += 1	
		print "%s,%s,%s,%d" %(user, item, session, event)	
	
def selectRandomFromList(list):
	return list[randint(0, len(list)-1)]
	
#load user and items
eventFile = sys.argv[1]
loadUsersAndItems(eventFile)

#start multiple session threads
try:
	for i in range(1,numSession):
		threadName = "session-%d" %(i)
		maxEvent = random.randrange(eventCountMin, eventCountMax)
   		thread.start_new_thread(sessionSimulate, (threadName, naxEvent, ) )
except:
   print "Error: unable to start thread"

while 1:
   pass
