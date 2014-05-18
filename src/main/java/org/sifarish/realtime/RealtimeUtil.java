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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.chombo.util.ConfigUtility;

import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;


/**
 * @author pranab
 *
 */
public class RealtimeUtil {
	
	public static Jedis buildRedisClient(Map stormConf) {
		String redisHost = ConfigUtility.getString(stormConf, "redis.server.host");
		int redisPort = ConfigUtility.getInt(stormConf,"redis.server.port");
		Jedis jedis = new Jedis(redisHost, redisPort);
		return jedis;
	}
	
	/**
	 * @param configFilePath
	 * @return
	 * @throws IOException
	 */
	public static Config buildStormConfig(String configFilePath) throws IOException {
        FileInputStream fis = new FileInputStream(configFilePath);
        Properties configProps = new Properties();
        configProps.load(fis);

        //initialize config
        Config conf = new Config();
        conf.setDebug(true);
        for (Object key : configProps.keySet()){
            String keySt = key.toString();
            String val = configProps.getProperty(keySt);
            conf.put(keySt, val);
        }
		
        return conf;
	}
	
	/**
	 * @param topologyName
	 * @param conf
	 * @param builder
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 */
	public static void submitStormTopology(String topologyName, Config conf,  TopologyBuilder builder) 
			throws AlreadyAliveException, InvalidTopologyException {
        int numWorkers = ConfigUtility.getInt(conf, "num.workers", 1);
        int maxSpoutPending = ConfigUtility.getInt(conf, "max.spout.pending", 1000);
        int maxTaskParalleism = ConfigUtility.getInt(conf, "max.task.parallelism", 100);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(maxSpoutPending);
        conf.setMaxTaskParallelism(maxTaskParalleism);
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
	}
	
}
