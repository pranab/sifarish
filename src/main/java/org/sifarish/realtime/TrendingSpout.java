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
 * @author pranab
 *
 */
public class TrendingSpout extends GenericSpout {
	private String eventQueue;
	private Jedis jedis;
	private int epochEvent;
	public static String EPOCH_STREAM = "epoch";
	public static String EVENT_STREAM = "event";
	
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
		eventQueue = ConfigUtility.getString(stormConf, "redis.event.queue");
		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
		if (debugOn) {
			LOG.setLevel(Level.INFO);;
		}
		epochEvent = ConfigUtility.getInt(stormConf, "epoch.event", 100);
	}

	@Override
	public MessageHolder nextSpoutMessage() {
		MessageHolder msgHolder = null;
		String message  = jedis.rpop(eventQueue);		
		if(null != message) {
			//message in event queue
			String[] items = message.split(",");
			
			//userID, sessionID, itemID, event
			int event = Integer.parseInt(items[3]);
			Values values = new Values(items[0], items[1], items[2], event);
			msgHolder = new  MessageHolder(values);
			msgHolder.setStream(event == epochEvent ? EPOCH_STREAM : EVENT_STREAM);
		}
		return msgHolder;
	}

	@Override
	public void handleFailedMessage(Values tuple) {
		// TODO Auto-generated method stub
		
	}

}
