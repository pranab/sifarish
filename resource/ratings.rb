#!/usr/bin/ruby

require '../lib/util.rb'      

itemCount = ARGV[0].to_i
userCount = ARGV[1].to_i
perItemUserCountMultipier = ARGV[2].to_i

avUserRatingPerItemCount = (userCount * perItemUserCountMultipier) / itemCount


itemCluster = Hash.new { |h,k| h[k] = [] }
userCluster = Hash.new { |h,k| h[k] = [] }


idGen = IdGenerator.new
1.upto userCount do
	userID = idGen.generate(12)
	hash = userID.hash % 10
	userCluster[hash] << userID
end
 
1.upto itemCount do
	itemID = idGen.generate(10)
	hash = itemID.hash % 10
	itemCluster[hash] << itemID
end
 
for c in 0..9 
	items = itemCluster[c]
	users = userCluster[c]
	items.each do |i|
		line = i
		numRating = (avUserRatingPerItemCount * 70)/100 + rand((avUserRatingPerItemCount * 60)/100)
		#puts "numRating: #{numRating}"
		#more exposed items
		if (rand(10) == 1)
			numRating = numRating < avUserRatingPerItemCount ? avUserRatingPerItemCount : numRating
			mult = 140 + rand(60)
			numRating = (mult * numRating) / 100
			#puts "more exposed numRating: #{numRating}"
		end
		
		unanimous = rand(10) < 2
		popular = rand(10) < 5
		#puts "unanimous: #{unanimous}  popular: #{popular}"
		1.upto numRating do
			if (rand(10) == 1)
				uc = c < 5 ? c + rand(10 -c) : rand(c)
				otherUsers = userCluster[uc]
				u = otherUsers[rand(otherUsers.length)]
			else 
				u = users[rand(users.length)]
			end
			if (unanimous)
				r = popular ? (60 + rand(40)) : (30 + rand(20))
			else
				r = 10 + rand(90)
			end
			line << ","
			line << u
			line << ":"
			line << r.to_s
		end
		puts line
	end 
end
