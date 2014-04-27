#!/usr/bin/ruby

require '../lib/util.rb'      
require 'securerandom'
require 'set'

itemCount = ARGV[0].to_i
custCount = ARGV[1].to_i
avEventCountPerCust = ARGV[2].to_i
eventCount = custCount * avEventCountPerCust
numItemPart = 20
itemPartSize = itemCount / numItemPart
itemIDs = []
custIDs = []
eventDist = NumericalFieldRange.new(3..3,6,4..4,9,5..5,15,6..6,10,7..7,11)
userSession = {}
eventMap = {}
novelItems = Set.new
novelItemsCount = itemCount * 0.05
popularlItems = Set.new
popularItemsCount = itemCount * 0.1
checkouts = []
shopCartEventLists = []
purchases = []
shopCartCount = 0
checkoutCount = 0
purchaseCount = 0
checkoutEventLists = []

# select item
def selectItem(itemIDs, itemCount, custID, novelItems, numItemPart, itemPartSize)
	noNovelItem = rand(10) < 6
	done = false
	while (!done) do
		if (rand(10) < 8)
			#select item from cluster
			itemPart = custID.hash % numItemPart
			itemIndx = itemPart * itemPartSize + rand(itemPartSize)
			itemID = itemIDs[itemIndx]
		else
			#select item randomly
			itemID = itemIDs[rand(itemCount)]
		end
		
		done = !(noNovelItem && novelItems.include?(itemID))
	end
	itemID
end 

# true if only browse events
def isBrowseOnly(events)
	browseOnly = true
	events.each do |e|
		if (e < 3)
			browseOnly = false
			break
		end
	end
	browseOnly
end

# true if shopping cart event exists
def inShoppingCart(events)
	netEventCount(events, 3) > 0
end

# true if shopping cart event exists
def inCheckout(events)
	netEventCount(events, 2) > 0
end

# true if net positive count for event
def netEventCount(events, event)
	count = 0
	events.each do |e|
		case e
			when event
			count = count + 1
			
			when -event
			count = count - 1
		end
	end
	count 
end

# select browse event
def selectBrowseEvent(events, eventDist)
	done = false
	while(!done)
		event = eventDist.value
		if (event == 3)
			exists = inShoppingCart(events)
			if (exists)
				if (rand(10) < 3)
					#remove from shopping cart
					event = -3
					done = true
				else
					# different browse event
					done = false
				end
			end
		else
			done = true
		end
	end
	event
end

#items
idGen = IdGenerator.new
1.upto itemCount do
	itemIDs << idGen.generate(10)
end

#user
1.upto custCount do
	custIDs << idGen.generate(12)
end

#novel items
while (novelItems.size < novelItemsCount) do
	itemID = itemIDs[rand(itemCount)]
	novelItems << itemID
end

timeGap = 20
now = Time.now.to_i
time = now - eventCount * (timeGap + 1)

#generate events
1.upto eventCount do
	#user
	custID = custIDs[rand(custCount)]
	if (userSession.key?(custID))
		sessionID = userSession[custID]	
	else 
		sessionID = SecureRandom.uuid	
		userSession[custID] = sessionID	
	end

	#item
	itemID = selectItem(itemIDs, itemCount, custID, novelItems, numItemPart, itemPartSize)
	
	# event list for custID, itemID
	eventKey = custID+itemID
	if (eventMap.has_key?(eventKey))
		eventList = eventMap[eventKey]	
	else
		eventList = []
		eventMap[eventKey] = eventList
	end	
	
	if (isBrowseOnly(eventList))
		#browse mode
		browseEventCount = 2 + rand(2)
		if (eventList.size >= browseEventCount)
			case rand(10)
			when 0..7
				if (inShoppingCart(eventList))
					#enter checkout if in shopping cart
					event = 2
				else
					# add to shopping cart
					event = 3
				end
			when 8..9
				#stay in browse			
				event = selectBrowseEvent(eventList, eventDist)
			end
		else 
			# stay in browse mode
			event = selectBrowseEvent(eventList, eventDist)
		end
	else 
		#in funnel
		if (inCheckout(eventList))
			case rand(10)
			when 0..6 
				#purchased
				event = 1	
			when 7..9
				#left checkout
				event = -2
			end		
		end
	end
	
	if (event.nil?) 
		event = selectBrowseEvent(eventList, eventDist)
	end
	eventList << event
	time = time + 14 + rand(6)
	puts "#{custID},#{sessionID},#{itemID},#{event},#{time}"
	if (event == 3)
		shopCartCount += 1
		checkouts << "#{custID},#{sessionID},#{itemID},#{event-1},#{time}"
		shopCartEventLists << eventList
	end
	
	#add some checkout events
	if ((rand(1000) < 100) && !checkouts.empty? && checkoutCount < (shopCartCount * 0.5))
		index = rand(checkouts.length)
		checkout = checkouts.delete_at(index)
		puts checkout
		shopCartEventLists[index] << 2
		checkoutCount += 1
		
		#create purchase event
		items = checkout.split(',')
		items[3] = 1
		purchase = items.join(',')
		purchases << purchase
		checkoutEventLists << shopCartEventLists[index]
	end
	
	#add some purchase events
	if ((rand(1000) < 100) && !purchases.empty? && purchaseCount < (checkoutCount * 0.8))
		index = rand(purchases.length)
		purchase = purchases.delete_at(index)
		puts purchase
		checkoutEventLists[index] << 1
		purchaseCount += 1
	end
	
end



