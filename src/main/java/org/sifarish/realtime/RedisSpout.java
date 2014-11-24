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

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

/**
 * Emits user engagement event data
 * @author pranab
 *
 */
public class RedisSpout  extends GenericSpout {
	private String eventQueue;
	private MessageQueue msgQueue;
	private static final String NIL = "nil";
	private static final Logger LOG = Logger.getLogger(RedisSpout.class);

	private static final long serialVersionUID = 7923992620587658921L;

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
		String redisHost = ConfigUtility.getString(stormConf, "redis.server.host");
		int redisPort = ConfigUtility.getInt(stormConf,"redis.server.port");
		eventQueue = ConfigUtility.getString(stormConf, "redis.event.queue");
		msgQueue = MessageQueue.createMessageQueue(stormConf, eventQueue);

		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
		if (debugOn) {
			LOG.setLevel(Level.INFO);;
		}
		messageCountInterval = ConfigUtility.getInt(stormConf,"log.message.count.interval", 100);
	}

	@Override
	public MessageHolder nextSpoutMessage() {
		MessageHolder msgHolder = null;
		String message  = msgQueue.receive();		
		if(null != message  && !message.equals(NIL)) {
			//message in event queue
			String[] items = message.split(",");
			
			//userID, sessionID, itemID, event, timestamp
			Values values = new Values(items[0], items[1], items[2], Integer.parseInt(items[3]), Long.parseLong(items[4]));
			msgHolder = new  MessageHolder(values);
			if (debugOn) {
				if (messageCounter % messageCountInterval == 0)
					LOG.info("event message - message counter:" + messageCounter );
			}
		}
		return msgHolder;
	}

	@Override
	public void handleFailedMessage(Values tuple) {
		// TODO Auto-generated method stub
	}

}
