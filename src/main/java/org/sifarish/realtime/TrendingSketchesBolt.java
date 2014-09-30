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

import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;
import org.chombo.util.ConfigUtility;
import org.hoidla.stream.BaseCountSketch;
import org.hoidla.stream.CountMinSketch;
import org.hoidla.stream.CountMinSketchesFrequent;
import org.hoidla.util.BoundedSortedObjects;
import org.hoidla.util.DailySchedule;
import org.hoidla.util.Expirer;
import org.hoidla.util.SimpleObjectCounter;
import org.hoidla.util.Utility;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Maintain a count min sketch
 * @author pranab
 *
 */
public class TrendingSketchesBolt extends  GenericBolt {
	private int tickFrequencyInSeconds;
	private CountMinSketchesFrequent sketches;
	private DailySchedule dailySchedule;
	private MessageHolder msg = new MessageHolder();
	public static final String BOLT_ID = "boltID";
	public static final String FREQ_COUNTS = "freqCounts";
	 
	enum ExpiryPolicy {
		None,
		Epoch,
		Tumble
	}
	private ExpiryPolicy expiryPolicy = ExpiryPolicy.None;
	private int tumbleTimeHour;
	private int ticksPerEpoch;
	private long tickCount;
	
	private static final Logger LOG = Logger.getLogger(TrendingSketchesBolt.class);
	private static final long serialVersionUID = 8844719835097201335L;

	
	/**
	 * @param tickFrequencyInSeconds
	 */
	public TrendingSketchesBolt(int tickFrequencyInSeconds) {
		super();
		this.tickFrequencyInSeconds = tickFrequencyInSeconds;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		  Config conf = new Config();
		  conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		  return conf;
	}

	@Override
	public void intialize(Map stormConf, TopologyContext context) {
		if (debugOn) {
			LOG.setLevel(Level.INFO);
			LOG.info("TrendingSketchesBolt intialization " );
		}
		//sketches object
		Expirer expirer = null;
		double errorLimit = ConfigUtility.getDouble(stormConf, "sketches.error.lim", 0.05);
		double errorProbLimit =ConfigUtility.getDouble(stormConf, "sketches.error.prob.limit", 0.02);
		int mostFrequentCount = ConfigUtility.getInt(stormConf, "sketches.most.freq.count", 3);
		int freqCountLimitPercent = ConfigUtility.getInt(stormConf, "sketches.freq.count.lim.percent", 20);
		String expiry = ConfigUtility.getString(stormConf, "sketches.expiry.policy", "none");
		if (expiry.equals("epoch")) {
			int maxEpoch = ConfigUtility.getInt(stormConf, "sketches.max.epoch", 5);
			expirer = new Expirer(maxEpoch);
			expiryPolicy = ExpiryPolicy.Epoch;
			ticksPerEpoch = ConfigUtility.getInt(stormConf, "sketches.epoch.size", 30) /  tickFrequencyInSeconds;
			LOG.info("ticksPerEpoch:" + ticksPerEpoch);
		} else if (expiry.equals("tumble")) {
			int[] tumbleTimeMin  = ConfigUtility.getIntArray(stormConf, "sketches.tumble.time.min");
			expiryPolicy = ExpiryPolicy.Tumble;
			dailySchedule = new DailySchedule(2*tickFrequencyInSeconds, tumbleTimeMin);
		}
		sketches = expirer == null ? 
				new CountMinSketchesFrequent(errorLimit, errorProbLimit, mostFrequentCount, freqCountLimitPercent) :
				new CountMinSketchesFrequent(errorLimit, errorProbLimit, mostFrequentCount, freqCountLimitPercent, expirer);
		boolean globalTotalCount = ConfigUtility.getBoolean(stormConf, "sketches.global.total.count", false);
		sketches.withGlobalTotalCount(globalTotalCount);
		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
	}

	@Override
	public boolean process(Tuple input) {
			boolean status = true;
			outputMessages.clear();
			if (isTickTuple(input)) {
				LOG.info("got tick tuple ");
				++tickCount;
				if (expiryPolicy == ExpiryPolicy.Epoch) {
					if (tickCount % ticksPerEpoch == 0) {
						LOG.info("going to expire sketches");
						sketches.expire();
						sketches.refreshCount();
					}
				} else if (expiryPolicy == ExpiryPolicy.Tumble) {
					if (dailySchedule.shouldTrigger()) {
						sketches.intialize();
					}
				}
				
				List<BoundedSortedObjects.SortableObject> topHitters = sketches.get();
				if (!topHitters.isEmpty()) {
					//send top hitters
					 LOG.info("sending top hitters to down stream bolt");
					String serFreqCounts = Utility.join(topHitters, ":");
					msg.setMessage( new Values(getID(), serFreqCounts));
					outputMessages.add(msg);
				}
		  } else {
			  String itemID = input.getStringByField(RecommenderBolt.ITEM_ID);
			  LOG.info("got message tuple ");
			  sketches.add(itemID);
		  }
		return status;
	}

	@Override
	public List<MessageHolder> getOutput() {
		return outputMessages;
	}

}
