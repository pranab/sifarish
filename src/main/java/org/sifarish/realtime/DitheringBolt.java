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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.storm.Cache;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;
import org.chombo.storm.MessageQueue;
import org.chombo.util.ConfigUtility;
import org.chombo.util.MathUtility;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * @author pranab
 *
 */
public class DitheringBolt extends  GenericBolt {
	private MessageQueue recQueue;
	private Cache recCache;
	private Cache ratingCache;
	private Map stormConf;
	private boolean writeRecommendationToQueue;
	private String recommendationQueue;
	private String recommendationCache;
	private static final Logger LOG = Logger.getLogger(DitheringBolt.class);
	private LoadingCache<String, List<UserItemRatings.ItemRating>> itemRatingCache = null;
	private String itemRatingKey;
	private int  ratingScale;
	private double ditherStdDev;
	
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void intialize(Map stormConf, TopologyContext context) {
		writeRecommendationToQueue = ConfigUtility.getBoolean(stormConf,"write.dithered.recomm.to.queue");
		if (writeRecommendationToQueue) {
			recommendationQueue = ConfigUtility.getString(stormConf, "redis.dithered.recomm.queue");
			recQueue = MessageQueue.createMessageQueue(stormConf, recommendationQueue);
		}else {
			recommendationCache = ConfigUtility.getString(stormConf, "redis.dithered.recomm.cache");
			recCache = Cache.createCache(stormConf, recommendationCache);
		}
		if (debugOn) {
			LOG.setLevel(Level.INFO);;
			LOG.info("bolt intialized " );
		}
		
		//initialize rating cache
		ratingScale = ConfigUtility.getInt(stormConf,"rating.scale", 100);
		itemRatingKey = ConfigUtility.getString(stormConf, "redis.item.rating.key");
		ratingCache = Cache.createCache(stormConf, itemRatingKey);
		
		int ratingCacheSize = ConfigUtility.getInt(stormConf,"rating.cache.size");
		int ratingCacheExpiryTimeSec = ConfigUtility.getInt(stormConf,"rating.cache.expiry.time.sec");
		if (null == itemRatingCache) {
			itemRatingCache = CacheBuilder.newBuilder()
					.maximumSize(ratingCacheSize)
				    .expireAfterAccess(ratingCacheExpiryTimeSec, TimeUnit.SECONDS)
				    .build(new ItemRatingLoader(ratingCache, ratingScale, debugOn));
		}
		
		ditherStdDev = ConfigUtility.getDouble(stormConf,"dither.std.dev");
	}

	@Override
	public boolean process(Tuple input) {
		boolean status = true;
		try {
			String user = input.getStringByField(RecommenderBolt.USER_ID);
			List<UserItemRatings.ItemRating> itemRatingListCached = itemRatingCache.get(user);
			List<UserItemRatings.ItemRating> itemRatingList = cloneItemRatingList(itemRatingListCached) ;
			
			dither(itemRatingList);
			
			//serialize ratings
			StringBuilder stBld = new StringBuilder(user);
			for (UserItemRatings.ItemRating itemRating : itemRatingList) {
				stBld.append(RecommenderBolt.FIELD_DELIM).append(itemRating.getItem()).
					append(RecommenderBolt.SUB_FIELD_DELIM).append(itemRating.getRating());
			}
			String itemRatings  = stBld.toString();

			//write to queue or cache
			if (writeRecommendationToQueue) {
				recQueue.send(itemRatings);
			} else {
				recCache.set(user, itemRatings);
			}
		} catch (ExecutionException e) {
			LOG.info("got error  " + e);
			status = false;
		}
		
		return status;
	}

	/**
	 * @param itemRatingListCached
	 * @return
	 */
	private List<UserItemRatings.ItemRating> cloneItemRatingList(List<UserItemRatings.ItemRating> itemRatingListCached) {
		List<UserItemRatings.ItemRating> itemRatingList = new ArrayList<UserItemRatings.ItemRating>();
		for (UserItemRatings.ItemRating itemRatingCached : itemRatingListCached) {
			UserItemRatings.ItemRating clonedItemRating = itemRatingCached.cloneItemRating();
			 itemRatingList.add(clonedItemRating);
		}		
		return itemRatingList;
	}
	
	@Override
	public List<MessageHolder> getOutput() {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * @param itemRatingList
	 */
	private void dither(List<UserItemRatings.ItemRating> itemRatingList) {
		int maxRating = 0;
		for (UserItemRatings.ItemRating itemRating : itemRatingList ) {
			//log of rating plus gaussian white noise
			 int rating   =(int) ((Math.log10( itemRating.getRating()) + MathUtility.sampleGaussian(0.0, ditherStdDev)) * ratingScale);
			 itemRating.setRating(rating);
				if (rating > maxRating) {
					maxRating = rating;
				}
		}
		
		//normalize
		for (UserItemRatings.ItemRating itemRating : itemRatingList ) {
			int rating =( itemRating.getRating() * ratingScale) / maxRating;
			itemRating.setRating(rating);
		}
		
		//sort
		Collections.sort(itemRatingList);
	}
	
	/**
	 * Cache loader for item correlation
	 * @author pranab
	 *
	 */
	private static class ItemRatingLoader extends CacheLoader<String, List<UserItemRatings.ItemRating>> {
		private Cache ratingCache;
		private boolean debugOn;
		private static final Logger LOG = Logger.getLogger(ItemRatingLoader.class);
		private int ratingScale;
		
		/**
		 * @param jedis
		 * @param itemRatingKey
		 * @param ratingScale
		 * @param debugOn
		 */
		public ItemRatingLoader(Cache ratingCache, int ratingScale, boolean debugOn) {
			this.ratingCache = ratingCache;
			this.debugOn = debugOn;
			this.ratingScale = ratingScale;
			if (debugOn) 
				LOG.setLevel(Level.INFO);
		}
		
		@Override
		public List<UserItemRatings.ItemRating> load(String user) throws Exception {
			List<UserItemRatings.ItemRating> itemRatingList = new ArrayList<UserItemRatings.ItemRating>();
			String ratings = ratingCache.get(user);
			ratings = ratings.trim();
			if (debugOn)
				LOG.info("user:" + user + " rating:" +ratings);
			String[] parts = ratings.split(",");
			int maxRating = 0;
			for (String part : parts) {
				String[] subParts = part.split(":"); 
				int rating = Integer.parseInt(subParts[1]);
				UserItemRatings.ItemRating itemRating = new UserItemRatings.ItemRating(subParts[0], rating);
				if (rating > maxRating) {
					maxRating = rating;
				}
				itemRatingList.add(itemRating);
			}
			
			//normalize
			for (UserItemRatings.ItemRating itemRating : itemRatingList ) {
				int rating =( itemRating.getRating() * ratingScale) / maxRating;
				itemRating.setRating(rating);
			}
			return itemRatingList;
		}
	}
}
