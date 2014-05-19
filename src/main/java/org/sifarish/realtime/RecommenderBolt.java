/*
 * Sifarish: Recommendation Engine
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.sifarish.realtime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;
import org.chombo.util.ConfigUtility;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * Makes real time recommendation based collaborative filtering
 * @author pranab
 *
 */
public class RecommenderBolt extends GenericBolt {
	private Jedis jedis;
	private Map<String, UserItemRatings> userItemRatings = new HashMap<String, UserItemRatings>();
	private Map stormConf;
	private boolean writeRecommendationToQueue;
	private String recommendationQueue;
	private String recommendationCache;
	
	public static final String USER_ID = "userID";
	public static final String SESSION_ID = "sessionID";
	public static final String ITEM_ID = "itemID";
	public static final String EVENT_ID = "eventID";
	public static final String TS_ID = "timeStamp";
	public static final String FIELD_DELIM = ",";
	public static final String SUB_FIELD_DELIM = ":";
	

	private static final Logger LOG = Logger.getLogger(RecommenderBolt.class);
	private static final long serialVersionUID = -8339901835031581335L;

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void intialize(Map stormConf, TopologyContext context) {
		String redisHost = ConfigUtility.getString(stormConf, "redis.server.host");
		int redisPort = ConfigUtility.getInt(stormConf,"redis.server.port");
		jedis = new Jedis(redisHost, redisPort);
		this.stormConf = stormConf;
		writeRecommendationToQueue = ConfigUtility.getBoolean(stormConf,"write.recommendation.to.queue");
		if (writeRecommendationToQueue) {
			recommendationQueue = ConfigUtility.getString(stormConf, "redis.recommendation.queue");
		}else {
			recommendationCache = ConfigUtility.getString(stormConf, "redis.recommendation.cache");
		}
		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
		if (debugOn) {
			LOG.setLevel(Level.INFO);;
			LOG.info("bolt intialized " );
		}
	
	}

	@Override
	public boolean process(Tuple input) {
		boolean status = true;
		String userID = input.getStringByField(USER_ID);
		String sessionID = input.getStringByField(SESSION_ID);
		String itemID = input.getStringByField(ITEM_ID);
		int eventID = input.getIntegerByField(EVENT_ID);
		long timeStamp = input.getLongByField(TS_ID);
		if (debugOn) 
			LOG.info("got tuple:" + userID + " " + sessionID + " " + itemID + " " + eventID + " " + timeStamp);
		
		try {
			UserItemRatings itemRatings = userItemRatings.get(userID);
			if (null == itemRatings) {
				itemRatings = new UserItemRatings(userID, sessionID, jedis, stormConf);
				userItemRatings.put(userID, itemRatings);
			}
			
			//add event and get predicted ratings
			itemRatings.addEvent(sessionID, itemID, eventID, timeStamp / 1000);
			List<UserItemRatings.ItemRating> predictedRatings = itemRatings.getPredictedRatings();
			if (debugOn) {
				LOG.info("num of recommended items:" + predictedRatings.size());
			}
			
			//write to queue or cache
			StringBuilder stBld = new StringBuilder(userID);
			for (UserItemRatings.ItemRating itemRating : predictedRatings) {
				stBld.append(FIELD_DELIM).append(itemRating.getItem()).append(SUB_FIELD_DELIM).
					append(itemRating.getRating());
			}
			String itemRatingList  = stBld.toString();
			if (writeRecommendationToQueue) {
				jedis.lpush(recommendationQueue, itemRatingList);
				if (debugOn) {
					LOG.info("wrote to recommendation queue");
				}
			} else {
				jedis.hset(recommendationCache, userID, itemRatingList);
			}
			
		} catch (Exception e) {
			//TODO
			LOG.info("got error  " + e);
			status = false;
		}
		return status;
	}

	@Override
	public List<MessageHolder> getOutput() {
		// TODO Auto-generated method stub
		return null;
	}

}
