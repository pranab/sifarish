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

import org.apache.log4j.Logger;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * @author pranab
 *
 */
public class TrendingAggregateBolt extends  GenericBolt {
	private Set<String> sketchedBolts = new HashSet<String>();
	private Map<String, Integer> frequentItems = new HashMap<String, Integer>();
	
	private static final Logger LOG = Logger.getLogger(TrendingAggregateBolt.class);
	private static final long serialVersionUID = 8275719621097842135L;

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void intialize(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean process(Tuple input) {
		  String sketchesBoltID = input.getStringByField(TrendingSketchesBolt.BOLT_ID);
		  if (sketchedBolts.contains(sketchesBoltID)) {
			  String freqItems = input.getStringByField(TrendingSketchesBolt.FREQ_COUNTS);
			  String[] parts = freqItems.split(":");
			  for (int i = 0; i < parts.length; i += 2) {
				  String itemID = parts[i];
				  int count = Integer.parseInt(parts[i+1]);
			  }
		  }
		  return false;
	}

	@Override
	public List<MessageHolder> getOutput() {
		// TODO Auto-generated method stub
		return null;
	}

}
