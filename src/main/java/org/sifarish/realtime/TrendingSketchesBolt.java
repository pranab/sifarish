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

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * @author pranab
 *
 */
public class TrendingSketchesBolt extends  GenericBolt {
	private static final Logger LOG = Logger.getLogger(TrendingSketchesBolt.class);
	private static final long serialVersionUID = 8844719835097201335L;

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void intialize(Map stormConf, TopologyContext context) {
		// TODO 
		
		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
		if (debugOn) {
			LOG.setLevel(Level.INFO);;
			LOG.info("TrendingSketchesBolt intialized " );
		}
	}

	@Override
	public boolean process(Tuple input) {
		String itemID = input.getStringByField(RecommenderBolt.ITEM_ID);
		int eventID = input.getIntegerByField(RecommenderBolt.EVENT_ID);
		String stream = input.getSourceStreamId();
		LOG.info("got message from stream:" + stream);
		if (stream.equals(TrendingSpout.EVENT_STREAM)) {

		} else {
			
		}
		return false;
	}

	@Override
	public List<MessageHolder> getOutput() {
		// TODO Auto-generated method stub
		return null;
	}

}
