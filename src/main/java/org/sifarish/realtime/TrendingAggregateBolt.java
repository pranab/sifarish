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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;
import org.chombo.storm.MessageQueue;
import org.chombo.util.ConfigUtility;
import org.hoidla.util.BoundedSortedObjects;
import org.hoidla.util.Utility;
import org.hoidla.util.BoundedSortedObjects.SortableObject;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * @author pranab
 * Aggregates frequent item counts from multiple bolts
 *
 */
public class TrendingAggregateBolt extends  GenericBolt {
	private Set<String> sketchedBolts = new HashSet<String>();
	private Map<String, Integer> frequentItems = new HashMap<String, Integer>();
	private int numSkethesBolt;
	private BoundedSortedObjects sortedObjects;
	private MessageQueue topHittersMsgQueue;
	private String topHittersQueue;
	
	private static final Logger LOG = Logger.getLogger(TrendingAggregateBolt.class);
	private static final long serialVersionUID = 8275719621097842135L;

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void intialize(Map stormConf, TopologyContext context) {
		numSkethesBolt = ConfigUtility.getInt(stormConf, "sketches.bolt.threads", 1);		
		int mostFrequentCount = ConfigUtility.getInt(stormConf, "sketches.most.freq.count", 3);
		sortedObjects  = new  BoundedSortedObjects(mostFrequentCount);
		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
		
		topHittersQueue = ConfigUtility.getString(stormConf, "redis.top.hitters.queue");
		topHittersMsgQueue = MessageQueue.createMessageQueue(stormConf, topHittersQueue);

		if (debugOn) {
			LOG.setLevel(Level.INFO);;
			LOG.info("TrendingAggregateBolt intialized " );
		}
		
	}

	@Override
	public boolean process(Tuple input) {
		boolean status = true;
		  String sketchesBoltID = input.getStringByField(TrendingSketchesBolt.BOLT_ID);
		  if (sketchedBolts.contains(sketchesBoltID)) {
			  //all bolts have not reported for current epoch
			  LOG.info("bolts didn't synchronize");
			  sketchedBolts.clear();
			  frequentItems.clear();
		  } 
		  
		  String freqItems = input.getStringByField(TrendingSketchesBolt.FREQ_COUNTS);
		  String[] parts = freqItems.split(":");
		  for (int i = 0; i < parts.length; i += 2) {
			  String itemID = parts[i];
			  int count = Integer.parseInt(parts[i+1]);
			  Integer curCount = frequentItems.get(itemID);
			  if (null == curCount) {
				  frequentItems.put(itemID, count);
			  } else {
				  frequentItems.put(itemID, curCount + count);
			  }
		  }
		  sketchedBolts.add(sketchesBoltID);
		  
		  //all bolts have joined
		  if (sketchedBolts.size() == numSkethesBolt) {
			  LOG.info("**Frequent item stats***");
			  sortedObjects.clear();
			  for (Map.Entry<String, Integer> entry : frequentItems.entrySet()) {
				  LOG.info("item:" + entry.getKey() + " count:" + entry.getValue());
				  sortedObjects.add(entry.getKey(), entry.getValue());
			  }
			  LOG.info("after merge");
			  sortedObjects.truncate();
			  List<SortableObject> topHitters = sortedObjects.get();
			  for (SortableObject topHitter : topHitters) {
				  LOG.info("item:" +  topHitter.getItem() + " count:" +  topHitter.getRank());
			  }
			  
			  //write to Redis
			  String serFreqCounts = Utility.join(topHitters, ":");
			  topHittersMsgQueue.send(serFreqCounts);
			  frequentItems.clear();
			  sketchedBolts.clear();
		  }
		  
		  return status;
	}

	@Override
	public List<MessageHolder> getOutput() {
		// TODO Auto-generated method stub
		return null;
	}

}
