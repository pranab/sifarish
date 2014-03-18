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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.chombo.util.ConfigUtility;
import org.chombo.util.Pair;

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
	private LoadingCache<String, ItemCorrelation> itemCorrelationCache = null;
	private int topItemsCount;
	/**
	 * @param userID
	 * @param sessionID
	 */
	public UserItemRatings(String userID, String sessionID, Map config) {
		super();
		this.userID = userID;
		this.sessionID = sessionID;
		
		//config
		int correlationCacheSize = ConfigUtility.getInt(config,"correlation.cache.size");
		int correlationCacheExpiryTime = ConfigUtility.getInt(config,"correlation.cache.expiry.time");
		topItemsCount = ConfigUtility.getInt(config,"top.items.count");
		
		//intialize correlaytion cache
		if (null == itemCorrelationCache) {
			itemCorrelationCache = CacheBuilder.newBuilder()
					.maximumSize(correlationCacheSize)
				    .expireAfterAccess(correlationCacheExpiryTime, TimeUnit.MINUTES)
				    .build(
				    		new CacheLoader<String, ItemCorrelation>() {
				             public ItemCorrelation load(String key) throws Exception {
				               return null;
				             }
				           });			
		}
	}
	
	/**
	 * @param sessionID
	 * @param itemID
	 * @param event
	 * @param timestamp
	 */
	public void addEvent(String sessionID, String itemID, int event, long timestamp) {
		if (this.sessionID != null && !this.sessionID.equals(sessionID)) {
			engagementEvents.clear();
			this.sessionID = sessionID;
		}
		EngagementEvent engageEvents = engagementEvents.get(itemID);
		engageEvents.addEvent(event, timestamp);
		engageEvents.processRating();
	}
	
	/**
	 * gets predicted ratings
	 * @return
	 */
	public List<ItemRating> getPredictedRatings() {
		List<ItemRating> ratings = new ArrayList<ItemRating>();
		Map<String, Integer> itemPredictedRatings = new HashMap<String, Integer>();
		Map<String, Integer> itemPredictedRatingCounts = new HashMap<String, Integer>();
		
		//all rated items
		for (String itemID : engagementEvents.keySet()) {
			EngagementEvent engageEvents = engagementEvents.get(itemID);
			List<ItemRating> thisPredictedRatings = engageEvents.getPredictedRatings();
			
			//all correlated items
			for (ItemRating itemRating : thisPredictedRatings) {
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
		
		Collections.sort(ratings);
		return ratings;
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
		
		@Override
		public int compareTo(ItemRating that) {
			return that.getRight().compareTo(this.getRight());
		}
		
	}

	/**
	 * Item and rating
	 * @author pranab
	 *
	 */
	public static class ItemCorrelation extends Pair<String, Integer>  {
		public ItemCorrelation(String itemID, int rating) {
			super(itemID, rating);
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
		private String itemID;
		private List<Pair<Integer, Long>> events = new ArrayList<Pair<Integer, Long>>();
		private int rating;
		private List<ItemRating> predictedRatings = new ArrayList<ItemRating>();
		
		public EngagementEvent(String itemID) {
			super();
			this.itemID = itemID;
		}
		
		public void addEvent(int event, long timestamp) {
			events.add(new Pair<Integer, Long>(event, timestamp));
		}	
		
		public void processRating() {
			//estimate imlpicit rating
			int rating = 0;
			
			//get correlated items
			
			
			//predict ratings
		}

		public List<ItemRating> getPredictedRatings() {
			return predictedRatings;
		}
	}
}
