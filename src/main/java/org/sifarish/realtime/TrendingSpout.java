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
import org.chombo.storm.MessageQueue;
import org.chombo.util.ConfigUtility;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

/**
 * @author pranab
 *
 */
public class TrendingSpout extends GenericSpout {
	private String eventQueue;
	private MessageQueue msgQueue;

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
		eventQueue = ConfigUtility.getString(stormConf, "redis.event.queue");
		msgQueue = MessageQueue.createMessageQueue(stormConf, eventQueue);

		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
		if (debugOn) {
			LOG.setLevel(Level.INFO);;
		}
	}

	@Override
	public MessageHolder nextSpoutMessage() {
		MessageHolder msgHolder = null;
		String message  = msgQueue.receive();		

		if(null != message) {
			//message in event queue
			String[] items = message.split(",");
			
			//userID, sessionID, itemID, event
			int event = Integer.parseInt(items[3]);
			Values values = new Values(items[0], items[1], items[2], event);
			msgHolder = new  MessageHolder(values);
		}
		return msgHolder;
	}

	@Override
	public void handleFailedMessage(Values tuple) {
		// TODO Auto-generated method stub
		
	}

}
