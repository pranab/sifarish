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
import backtype.storm.tuple.Fields;

/**
 * Rating dithering storm topology
 * @author pranab
 *
 */
public class DitheringTopology {

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
        DitheringSpout spout  = new DitheringSpout();
        spout.withTupleFields(RecommenderBolt.USER_ID);
        builder.setSpout("ditheringRedisSpout", spout, spoutThreads);    
        
        //bolt
        DitheringBolt bolt = new DitheringBolt();
        int boltThreads = ConfigUtility.getInt(conf, "bolt.threads", 1);
        boolean globalDithering = ConfigUtility.getBoolean(conf, "global.dithering", true);
        if (globalDithering) {
        	//shuffle grouping
            builder.
        	setBolt("ditheringrBolt", bolt, boltThreads).
        	shuffleGrouping("ditheringRedisSpout");
       } else {
    	   //field grouping by userID
            builder.
            	setBolt("ditheringrBolt", bolt, boltThreads).
            	fieldsGrouping("ditheringRedisSpout", new Fields(RecommenderBolt.USER_ID));
        }
    	
        //submit
        RealtimeUtil.submitStormTopology(topologyName, conf,  builder);
    }
}
