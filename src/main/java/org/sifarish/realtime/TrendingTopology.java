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

import org.chombo.util.ConfigUtility;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author pranab
 *
 */
public class TrendingTopology {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
    		throw new IllegalArgumentException("Need two arguments: topology name and config file path");
    	}
    	String topologyName = args[0];
    	String configFilePath = args[1];
    	Config conf = RealtimeUtil.buildStormConfig(configFilePath);
    	
    	//spout
        TopologyBuilder builder = new TopologyBuilder();
        int spoutThreads = ConfigUtility.getInt(conf, "spout.threads", 1);
        TrendingSpout spout  = new TrendingSpout();
        spout.withTupleFields(RecommenderBolt.USER_ID, RecommenderBolt.SESSION_ID, 
        		RecommenderBolt.ITEM_ID, RecommenderBolt.EVENT_ID);
        builder.setSpout("trendingRedisSpout", spout, spoutThreads);
        
        //sketches bolt
        int tickFrequencyInSeconds = ConfigUtility.getInt(conf, "tick.freq.sec", 1);
        TrendingSketchesBolt sketchesBolt = new TrendingSketchesBolt(tickFrequencyInSeconds);
        sketchesBolt.withTupleFields(TrendingSketchesBolt.BOLT_ID, TrendingSketchesBolt.FREQ_COUNTS);
        int boltThreads = ConfigUtility.getInt(conf, "sketches.bolt.threads", 1);
        builder.
        	setBolt("trendingSketchesBolt", sketchesBolt, boltThreads).
        	shuffleGrouping("trendingRedisSpout");
        
        //trending aggregation bolt
        TrendingAggregateBolt  aggrBolt = new TrendingAggregateBolt();
        boltThreads = ConfigUtility.getInt(conf, "aggr.bolt.threads", 1);
        builder.
    		setBolt("trendingAggrBolt", aggrBolt, boltThreads).
    		shuffleGrouping("trendingSketchesBolt");
        
        //submit
        RealtimeUtil.submitStormTopology(topologyName, conf,  builder);
    }
}
