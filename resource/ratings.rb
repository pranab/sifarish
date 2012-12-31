require '../lib/util.rb'      

itemCount = ARGV[0].to_i
userCount = ARGV[1].to_i
perUserItemCountMultipier = ARGV[2].to_i

avItemPerUser = (itemCount * perUserItemCountMultipier) / userCount


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
		numRating = 5 + rand(avItemPerUser)
		1.upto numRating do
			if (rand(10) == 1)
				uc = c < 5 ? c + rand(10 -c) : rand(c)
				otherUsers = userCluster[uc]
				u = otherUsers[rand(otherUsers.length)]
			else 
				u = users[rand(users.length)]
			end
			r = 1 + rand(5)
			line << ","
			line << u
			line << ":"
			line << r.to_s
		end
		puts line
	end 
end
