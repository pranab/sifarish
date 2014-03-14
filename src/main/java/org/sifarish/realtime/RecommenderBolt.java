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

import org.apache.log4j.Logger;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * Makes real time recommendation based collaborative filtering
 * @author pranab
 *
 */
public class RecommenderBolt extends GenericBolt {
	public static final String USER_ID = "userID";
	public static final String SESSION_ID = "sessionID";
	public static final String ITEM_ID = "itemID";
	public static final String EVENT_ID = "eventID";

	private static final Logger LOG = Logger.getLogger(RecommenderBolt.class);
	private static final long serialVersionUID = -8339901835031581335L;

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
		String userID = input.getStringByField(USER_ID);
		String sessionID = input.getStringByField(SESSION_ID);
		String itemID = input.getStringByField(ITEM_ID);
		int eventID = input.getIntegerByField(EVENT_ID);
		
		return false;
	}

	@Override
	public List<MessageHolder> getOutput() {
		// TODO Auto-generated method stub
		return null;
	}

}
