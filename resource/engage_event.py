#!/usr/bin/python

import sys
from random import randint
import time
import redis
import uuid
import threading

numSession = 2
eventCountMin = 20
eventCountMax = 40

#key : user value: list of items
userItems = {}

allItems = []
lowEngaeEvents = [2,3,4]
midEngageEvents = [1]
terminalEvent = 0

rc = redis.StrictRedis(host='localhost', port=6379, db=0)

# load user, item and event file
def loadUsersAndItems(eventFile):
	itemSet = set()
	file = open(eventFile, 'r')

	#read file
	for line in file:
		line.strip()
		tokens = line.split(',')	
		user = tokens[0]
		items = userItems.get(user)
		if items is None:
			items = [tokens[1]]
			userItems[user] = items
		else:
			items.append(tokens[1])
		
		itemSet.add(tokens[1])
		
	file.close()

	#generate items list
	for it in itemSet:
		allItems.append(it)
		
	print "loaded event history with %d users and %d items" %(len(userItems.keys()), len(allItems))
	
# generates (userID, itemID, sessionID, event, time) tuples	
def genEngageEvents(threadName, maxEvent):
	engagedItems = {}
	
	#choose user
	users = userItems.keys()
	user = selectRandomFromList(users)
	print "starting %s with max event %d for user %s" %(threadName, maxEvent, user)
	
	#sessionID
	session = uuid.uuid1()
	
	evCount = 0
	done = False
	while (evCount < maxEvent and not done):
		if (randint(0,9) < 7 and  len(engagedItems) > 2):
			#choose item already engaged with in this session
			item = selectRandomFromList(engagedItems.keys())
		else:
			if (randint(0,9) < 7):
				#choose an item from past history
				item = selectRandomFromList(userItems[user])
			else:
				#choose something new
				item = selectRandomFromList(allItems)
				
		#choose event type
		events = engagedItems.get(item)
		if (events is None):
			#not engaged before
			event = selectRandomFromList(lowEngaeEvents)
			events = set()
			engagedItems[item] = events
		else:
			#engaged before
			if 0 in events:
				#terminal event
				done = True
			elif 1 in events:
				if (randint(0,9) < 7):
					event = 0
					done = True
				else:
					event = selectRandomFromList(lowEngaeEvents)
			else:
				if (randint(0,9) < 7):
					event = selectRandomFromList(lowEngaeEvents)
				else:
					event = 1

		events.add(event)				
		evCount += 1	
		#print "%s,%s,%s,%d" %(user, item, session, event)
		event = "%s,%s,%s,%d" %(user, item, session, event)
		rc.lpush("engageEventQueue", event)
		print "thread %s generated event" %(threadName)
		time.sleep(randint(2,6))
	
def selectRandomFromList(list):
	return list[randint(0, len(list)-1)]

#browse engagement event queue
def showEventQueue():
	while True:
		line = rc.rpop("engageEventQueue")
		if line is not None:
			print line
		else:
			break
	
#loads items correlation data into redis
def loadCorrelation(corrFile):
	#read file
	file = open(corrFile, 'r')
	for line in file:
		line.strip()
		index = line.find(",")
		key = line[0:index]
		val = line[index+1:]
		rc.hset("itemCorrelation", key, val)		

#load event mapping to redis
def loadEventMapping(eventMappingFile):
	file = open(eventMappingFile, 'r')
	mappingData = file.read()
	rc.set('eventMappingMetadata', mappingData)
	
#command processing
op = sys.argv[1]
if (op == "genEvents"):	            
	#load user and items
	eventFile = sys.argv[2]
	loadUsersAndItems(eventFile)

	#start multiple session threads
	try:
		for i in range(numSession):
			threadName = "session-%d" %(i)
			maxEvent = randint(eventCountMin, eventCountMax)
   			t = threading.Thread(target=genEngageEvents, args=(threadName,maxEvent, ))
   			t.start()
	except:
   		print "Error: unable to start thread"
elif (op == "showEvents"):
	showEventQueue()
elif (op == "loadCorrelation"):
	corrFile = sys.argv[2]
	loadCorrelation(corrFile)
elif (op == "loadEventMapping"):
	corrFile = sys.argv[2]
	loadEventMapping(eventMappingFile)	
	
