#!/usr/bin/ruby

require '../lib/util.rb'      
require 'securerandom'

itemCount = ARGV[0].to_i
custCount = ARGV[1].to_i
avEventCountPerCust = ARGV[2].to_i
eventCount = custCount * avEventCountPerCust

numItemPart = 20
itemPartSize = itemCount / numItemPart


itemIDs = []
custIDs = []
eventDist = NumericalFieldRange.new(0..0,2,1..1,3,2..2,6,3..3,9,4..4,15,5..5,10,6..6,11)
userSession = {}

idGen = IdGenerator.new
1.upto itemCount do
	itemIDs << idGen.generate(10)
end

1.upto custCount do
	custIDs << idGen.generate(12)
end

timeGap = 20
now = Time.now.to_i
time = now - eventCount * (timeGap + 1)


1.upto eventCount do
	custID = custIDs[rand(custCount)]
	if (userSession.key?(custID))
		sessionID = userSession[custID]	
	else 
		sessionID = SecureRandom.uuid	
		userSession[custID] = sessionID	
	end
	
	if (rand(10) < 8)
		#select item from cluster
		itemPart = custID.hash % numItemPart
		itemIndx = itemPart * itemPartSize + rand(itemPartSize)
		itemID = itemIDs[itemIndx]
	else
		#select item randomly
		itemID = itemIDs[rand(itemCount)]
	end
	event = eventDist.value
	time = time + 14 + rand(6)
	puts "#{custID},#{sessionID},#{itemID},#{event},#{time}"
end



