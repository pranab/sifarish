#!/usr/bin/python

import os
import sys
from random import randint
import time
import redis
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *

eventCountMin = 20
eventCountMax = 40
users = []
items = []
events = [1,2,3,4,5]
rc = redis.StrictRedis(host='localhost', port=6379, db=0)
epochInterval = 10

def genEpochEvent(threadName, interval, numEvent):
	print "starting epoch thread with interval %d and number of events %d" %(interval,numEvent)
	epochTime = int(time.time())
	remainder = interval - (epochTime % interval)
	time.sleep(remainder/1000)
	session = uuid.uuid1()
	
	for i in range(numEvent):
		eventRec = "XXXXXXXXXXXX,%s,XXXXXXXX,100" %(session)
		rc.lpush("engageEventQueue", eventRec)	
		print "sent epoch event"
		time.sleep(interval)
	
# generates (userID, itemID, sessionID, event, time) tuples	
def genEngageEvents(threadName, users, items, trendingItems, maxEvent, events):
	engagedItems = {}
	
	#choose user
	user = selectRandomFromList(users)
	print "starting %s with max event %d for user %s" %(threadName, maxEvent, user)
	numItemsForThisUser = randint(5, 20)
	itemsForThisUser = selectRandomSubListFromList(items, numItemsForThisUser)
	 
	#sessionID
	session = uuid.uuid1()

	for i in range(maxEvent):
		if (randint(0,9) < 7):
			item = selectRandomFromList(itemsForThisUser)
		else:
			item = selectRandomFromList(trendingItems)
		event = selectRandomFromList(events)
		eventRec = "%s,%s,%s,%d" %(user,session,item,event)
		rc.lpush("engageEventQueue", eventRec)
		print "thread %s generated event" %(threadName)
		print eventRec
		time.sleep(randint(2,6))


########################### command processing #########################	
op = sys.argv[1]
if (op == "genEvents"):	            
	#start multiple session threads
	numUsers = int(sys.argv[2])
	numItems = int(sys.argv[3])
	numSession = int(sys.argv[4])
	print "numUsers %d numItems %d numSession %d" %(numUsers, numItems, numSession)
	#generate user IDs
	for i in range(numUsers):
		users.append(genID(12))
	

	#generate item IDs
	for i in range(numItems):
		id = genID(8)
		print id
		items.append(id)

	#trending items
	trendingItems = selectRandomSubListFromList(items, 5)	
	
	try:
   		#t = threading.Thread(target=genEpochEvent, args=("epoch event thread",epochInterval,20, ))
   		#t.start()
	
		for i in range(numSession):
			threadName = "user session-%d" %(i)
			maxEvent = randint(eventCountMin, eventCountMax)
   			t = threading.Thread(target=genEngageEvents, args=(threadName,users,items,trendingItems,maxEvent,events, ))
   			t.start()
			time.sleep(randint(2,4))

	except:
   		print "Error: unable to start thread"


