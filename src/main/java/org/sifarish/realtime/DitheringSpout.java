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

import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.storm.GenericSpout;
import org.chombo.storm.MessageHolder;
import org.chombo.util.ConfigUtility;

import redis.clients.jedis.Jedis;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

/**
 * Consumes an user ID. For globalis used. recommendation a special userID. Passes userID to
 * bolts to dither recommendations for the user 
 * @author pranab
 *
 */
public class DitheringSpout  extends GenericSpout  {
	private String eventQueue;
	private Jedis jedis;
	private static final String NIL = "nil";
	private static final Logger LOG = Logger.getLogger(DitheringSpout.class);

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void intialize(Map stormConf, TopologyContext context) {
		jedis = RealtimeUtil.buildRedisClient(stormConf);
		eventQueue = ConfigUtility.getString(stormConf, "redis.userID.queue");
		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
		if (debugOn) {
			LOG.setLevel(Level.INFO);;
		}
	}

	@Override
	public MessageHolder nextSpoutMessage() {
		MessageHolder msgHolder = null;
		String message  = jedis.rpop(eventQueue);		
		if(null != message  && !message.equals(NIL)) {
			//message in event queue
			Values values = new Values(message);
			msgHolder = new  MessageHolder(values);
		}
		return msgHolder;
	}

	@Override
	public void handleFailedMessage(Values tuple) {
		// TODO Auto-generated method stub
		
	}

}
