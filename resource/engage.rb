#!/usr/bin/ruby

require '../lib/util.rb'      


custCount = ARGV[0].to_i
itemCount = ARGV[1].to_i
eventCount = ARGV[2].to_i

itemIDs = []
custIDs = []
eventDist = NumericalFieldRange.new(0..0,2,1..1,3,2..2,6,3..3,9,4..4,10)

idGen = IdGenerator.new
1.upto itemCount do
	itemIDs << idGen.generate(10)
end

1.upto custCount do
	custIDs << idGen.generate(12)
end

timeGap = 20
now = Time.now.to_i
time = now - eventCount * timeGap


1.upto eventCount do
	custID = custIDs[rand(custCount)]
	itemID = itemIDs[rand(itemCount)]
	event = eventDist.value
	time = time + 14 + rand(6)
	puts "#{custID},#{itemID},#{event},#{time}"
end



