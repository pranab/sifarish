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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.ConfigUtility;
import org.chombo.util.Pair;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.sifarish.common.EngagementToPreferenceMapper;

import redis.clients.jedis.Jedis;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Gets predicted ratings for all items correlated with items rated by this user
 * @author pranab
 *
 */
public class UserItemRatings {
	private String userID;
	private String sessionID;
	private Map<String, EngagementEvent>  engagementEvents = new HashMap<String, EngagementEvent>();
	private LoadingCache<String, List<ItemCorrelation>> itemCorrelationCache = null;
	private int topItemsCount;
	private String itemCorrelationKey;
	private EngagementToPreferenceMapper engaementMapper;
	private Jedis jedis;
	private String eventExpirePolicy;
	private long timedExpireWindowSec;
	private int countExpireLimit;
	private boolean debugOn;
	
	private static final String EVENT_EXPIRE_SESSION = "session";
	private static final String EVENT_EXPIRE_TIME = "time";
	private static final String EVENT_EXPIRE_COUNT = "count";

	private static final Logger LOG = Logger.getLogger(UserItemRatings.class);

	/**
	 * @param userID
	 * @param sessionID
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonParseException 
	 */
	public UserItemRatings(String userID, String sessionID, Jedis jedis, Map config) 
			throws Exception {
		super();
		this.userID = userID;
		this.sessionID = sessionID;
		this.jedis = jedis;
		
		//config
		int correlationCacheSize = ConfigUtility.getInt(config,"correlation.cache.size");
		int correlationCacheExpiryTimeSec = ConfigUtility.getInt(config,"correlation.cache.expiry.time.sec");
		topItemsCount = ConfigUtility.getInt(config,"top.items.count");
		itemCorrelationKey = ConfigUtility.getString(config, "redis.item.correlation.key");
		String eventMappingMetadataKey = ConfigUtility.getString(config, "redis.event.mapping.metadata.key");
		eventExpirePolicy = ConfigUtility.getString(config, "event.expire.policy", EVENT_EXPIRE_SESSION);
		timedExpireWindowSec = ConfigUtility.getLong(config, "timed.expire.window.sec", -1);
		countExpireLimit = ConfigUtility.getInt(config,"count.expire.limit", -1);
				
				
		//event mapping metadata
		String eventMappingMetadata = jedis.get(eventMappingMetadataKey);		
		ObjectMapper mapper = new ObjectMapper();
		engaementMapper = mapper.readValue(eventMappingMetadata, EngagementToPreferenceMapper.class);

		//log
		debugOn = ConfigUtility.getBoolean(config,"debug.on", false);
		
		//initialize correlation cache
		if (null == itemCorrelationCache) {
			itemCorrelationCache = CacheBuilder.newBuilder()
					.maximumSize(correlationCacheSize)
				    .expireAfterAccess(correlationCacheExpiryTimeSec, TimeUnit.SECONDS)
				    .build(new ItemCorrelationLoader(jedis, itemCorrelationKey, debugOn));
		}
		
		if (debugOn) {
			LOG.setLevel(Level.INFO);
			LOG.info("UserItemRatings intialized");
		}
		
	}
	
	/**
	 * @param sessionID
	 * @param itemID
	 * @param event
	 * @param timestamp
	 */
	public void addEvent(String sessionID, String itemID, int event, long timestamp) throws Exception {
		Set<String> affectedItems = new HashSet<String>();

		//add event
		EngagementEvent engageEvents = engagementEvents.get(itemID);
		if (null == engageEvents) {
			engageEvents = new EngagementEvent(itemID, itemCorrelationCache, engaementMapper, debugOn);
			engagementEvents.put(itemID, engageEvents);
		}
		engageEvents.addEvent(event, timestamp);
		affectedItems.add(itemID);
		if (debugOn) {
			LOG.info("event added to EngagementEvent");
		}
		
		//handle event expiry
		if (eventExpirePolicy.equals(EVENT_EXPIRE_SESSION))  {
			//expire by session
			if (this.sessionID != null && !this.sessionID.equals(sessionID)) {
				for (String item : engagementEvents.keySet()) {
					if (engagementEvents.get(item).removeAllEvents()) {
						affectedItems.add(item);
					}
				}
				this.sessionID = sessionID;
			}
		} else if (eventExpirePolicy.equals(EVENT_EXPIRE_TIME)) {
			//expire by time window
			if (timedExpireWindowSec < 0) {
				throw new Exception("For event expiry by time window, timed.expire.window needs to be set");
			}
			for (String item : engagementEvents.keySet()) {
				if (engagementEvents.get(item).removeOldEventsByTime(timedExpireWindowSec)) {
					affectedItems.add(item);
				}
			}
			
		} else if (eventExpirePolicy.equals(EVENT_EXPIRE_COUNT)) {
			//expire by max event list size
			if (countExpireLimit < 0) {
				throw new Exception("For event expiry by count, count.expire.limit needs to be set");
			}
			for (String item : engagementEvents.keySet()) {
				if (engagementEvents.get(item).removeOldEventsByCount(10)) {
					affectedItems.add(item);
				}
			}
		}
		if (debugOn) {
			LOG.info("Handled event expiry, num of affcted items:" + affectedItems.size());
		}
		
	}
	
	/**
	 * gets predicted ratings
	 * @return
	 * @throws Exception 
	 */
	public List<ItemRating> getPredictedRatings() throws Exception {
		List<ItemRating> ratings = new ArrayList<ItemRating>();
		Map<String, Integer> itemPredictedRatings = new HashMap<String, Integer>();
		Map<String, Integer> itemPredictedRatingCounts = new HashMap<String, Integer>();

		if (debugOn) {
			LOG.info("num of items user " + userID +  "  engaged with:"  + engagementEvents.size());
		}
		
		//all rated items
		for (String itemID : engagementEvents.keySet()) {
			//predicted ratings for items correlated to this item 
			if (debugOn)
				LOG.info("processing item:" +itemID);
			
			EngagementEvent engageEvents = engagementEvents.get(itemID);
			engageEvents.processRating();
			List<ItemRating> thisPredictedRatings = engageEvents.getPredictedRatings();
			
			if (debugOn) 
				LOG.info("for item "  + itemID + " there are  " + thisPredictedRatings.size() + " correlated items with predicted ratings");
			
			//all correlated items
			for (ItemRating itemRating : thisPredictedRatings) {
				//aggregate predicted ratings
				String item = itemRating.getItem();
				Integer rating = itemPredictedRatings.get(item);
				if (null == rating) {
					itemPredictedRatings.put(item, itemRating.getRating());
					itemPredictedRatingCounts.put(item, 1);
				} else {
					itemPredictedRatings.put(itemRating.getItem(), rating + itemRating.getRating());
					itemPredictedRatingCounts.put(item, itemPredictedRatingCounts.get(item) + 1);
				}
			}
		}
		
		//average predicted rating
		for (String item : itemPredictedRatings.keySet()) {
			int avRating = itemPredictedRatings.get(item) / itemPredictedRatingCounts.get(item);
			ratings.add(new ItemRating(item, avRating));
		}
		if (debugOn) {
			LOG.info("found net " + ratings.size() + " items with predicted rating");
		}		
		
		//sort and collect top n
		Collections.sort(ratings);
		if (ratings.size() > topItemsCount) {
			ratings.subList(topItemsCount, ratings.size()).clear();
			if (debugOn) {
				LOG.info("picked top k items");
			}		
		}
		return ratings;
	}
	
	/**
	 * Cache loader for item correlation
	 * @author pranab
	 *
	 */
	private static class ItemCorrelationLoader extends CacheLoader<String, List<ItemCorrelation>> {
		private String itemCorrelationKey;
		private Jedis jedis;
		private boolean debugOn;
		private static final Logger LOG = Logger.getLogger(ItemCorrelationLoader.class);
		
		public ItemCorrelationLoader(Jedis jedis, String itemCorrelationKey, boolean debugOn) {
			this.jedis = jedis;
			this.itemCorrelationKey = itemCorrelationKey;
			this.debugOn = debugOn;
			if (debugOn) 
				LOG.setLevel(Level.INFO);

		}
		
		@Override
		public List<ItemCorrelation> load(String item) throws Exception {
			List<ItemCorrelation> itemCorrList = new ArrayList<ItemCorrelation>();
			String correlation = jedis.hget(itemCorrelationKey, item);
			correlation = correlation.trim();
			if (debugOn)
				LOG.info("item:" + item + " correlation:" +correlation);
			String[] parts = correlation.split(",");
			for (String part : parts) {
				String[] subParts = part.split(":"); 
				ItemCorrelation itemCorr = new ItemCorrelation(subParts[0], Integer.parseInt(subParts[1]));
				itemCorrList.add(itemCorr);
			}
			return itemCorrList;
		}
	}
	
	/**
	 * Item and rating
	 * @author pranab
	 *
	 */
	public static class ItemRating extends Pair<String, Integer> implements  Comparable<ItemRating> {
		
		public ItemRating(String itemID, int rating) {
			super(itemID, rating);
		}

		public String getItem() {
			return getLeft();
		}
		
		public int getRating() {
			return getRight();
		}
		
		public void setRating(int rating) {
			setRight(rating);
		}
		
		@Override
		public int compareTo(ItemRating that) {
			return that.getRight().compareTo(this.getRight());
		}
		
		public ItemRating cloneItemRating() {
			return new ItemRating(this.left, this.right);
		}
	}

	/**
	 * Item and correlation
	 * @author pranab
	 *
	 */
	public static class ItemCorrelation extends Pair<String, Integer>  {
		public ItemCorrelation(String itemID, int correlation) {
			super(itemID, correlation);
		}

		public String getItem() {
			return getLeft();
		}
		
		public int getCorrelation() {
			return getRight();
		}
	}

	/**
	 * Engaegement events for an item
	 * @author pranab
	 *
	 */
	private static class EngagementEvent {
		private String item;
		private List<Pair<Integer, Long>> events = new ArrayList<Pair<Integer, Long>>();
		private int currentRating = -1;
		private List<ItemRating> predictedRatings = new ArrayList<ItemRating>();
		private LoadingCache<String, List<ItemCorrelation>> itemCorrelationCache;
		private EngagementToPreferenceMapper engaementMapper;
		private boolean debugOn;
		private static final Logger LOG = Logger.getLogger(EngagementEvent.class);
		

		/**
		 * @param item
		 * @param itemCorrelationCache
		 */
		public EngagementEvent(String item, LoadingCache<String, List<ItemCorrelation>> itemCorrelationCache, 
				EngagementToPreferenceMapper engaementMapper,  boolean debugOn) {
			super();
			this.item = item;
			this.itemCorrelationCache = itemCorrelationCache;
			this.engaementMapper = engaementMapper;
			this.debugOn = debugOn;
			if (debugOn) {
				LOG.setLevel(Level.INFO);
			}			
		}
		
		/**
		 * 
		 */
		public boolean removeAllEvents() {
			events.clear();
			predictedRatings.clear();
			currentRating = -1;
			return true;
		}
		
		/**
		 * @param timedExpireWindow
		 */
		public boolean removeOldEventsByTime(long timedExpireWindow) {
			boolean changed = false;
			long thresholdTime = System.currentTimeMillis() / 1000 - timedExpireWindow;
			
			List<Pair<Integer, Long>> filteredEvents = new ArrayList<Pair<Integer, Long>>();
			for (Pair<Integer, Long> event : events) {
				if (event.getRight() >= thresholdTime) {
					filteredEvents.add(event);
				}
			}
			
			if (debugOn) {
				LOG.info("event count:" + events.size() + " event count after filtering : " + filteredEvents.size());
			}
			if (filteredEvents.size() < events.size()) {
				events = filteredEvents;
				predictedRatings.clear();
				currentRating = -1;
				changed = true;
			}
			return changed;
		}

		/**
		 * @param timedExpireWindow
		 */
		public boolean removeOldEventsByCount(int maxCount) {
			boolean changed = false;
			if (events.size() > maxCount) {
				events.remove(0);
				predictedRatings.clear();
				currentRating = -1;
				changed = true;
			}
			return changed;
		}		
		/**
		 * @param event
		 * @param timestamp
		 */
		public void addEvent(int event, long timestamp) {
			events.add(new Pair<Integer, Long>(event, timestamp));
		}	
		
		/**
		 * @throws Exception
		 */
		public void processRating() throws Exception {
			//if predicted rating list is empty and there are events
			if (predictedRatings.isEmpty() && !events.isEmpty()) {
				if (debugOn) {
					LOG.info("going to find predicted ratings");
				}
				//most engaging event and corresponding count
				int mostEngaingEvent = 1000000;
				int eventCount = 0;
				for (Pair<Integer, Long> event : events) {
					if (event.getLeft() < mostEngaingEvent) {
						mostEngaingEvent = event.getLeft();
						eventCount = 1;
					} else if (event.getLeft() == mostEngaingEvent) {
						++eventCount;
					}
				}
				
				//estimate implicit rating 
				int rating = engaementMapper.scoreForEvent(mostEngaingEvent, eventCount);
				if (debugOn) 
					LOG.info("mostEngaingEvent:" + mostEngaingEvent + " eventCount:" + eventCount + " rating:" + rating );
				
				//predicted ratings only if first time or if rating is better that current
				if (currentRating < 0 || rating > currentRating) {
					//get correlated items
					List<ItemCorrelation> itemCorrs = itemCorrelationCache.get(item);
					if (debugOn) 
						LOG.info("number of correlated items:" + itemCorrs.size());
								
					//predict ratings
					for (ItemCorrelation itemCorr : itemCorrs) {
						ItemRating itemRating = new ItemRating(itemCorr.getItem(), itemCorr.getCorrelation() * rating);
						predictedRatings.add(itemRating);
					}
					currentRating = rating;
				}
			}
		}

		/**
		 * @return
		 */
		public List<ItemRating> getPredictedRatings() {
			return predictedRatings;
		}
	}
}
