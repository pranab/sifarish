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

package org.sifarish.util;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.sifarish.feature.SingleTypeSchema;

/**
 * Utility
 * @author pranab
 *
 */
public class Utility {
	public final static double AVERAGE_RADIUS_OF_EARTH = 6371;
	
    public static void setConfiguration(Configuration conf) throws Exception{
        String confFilePath = conf.get("conf.path");
        if (null != confFilePath){
            FileInputStream fis = new FileInputStream(confFilePath);
            Properties configProps = new Properties();
            configProps.load(fis);

            for (Object key : configProps.keySet()){
                String keySt = key.toString();
                conf.set(keySt, configProps.getProperty(keySt));
            }
        }
    }

    /**
     * @param loc1
     * @param loc2
     * @return
     */
    public static int getGeoDistance(String loc1, String loc2) {
    	String[] items = loc1.split(":");
    	double lat1 = Double.parseDouble(items[0]);
    	double long1 = Double.parseDouble(items[1]);
    	
    	items = loc2.split(":");
    	double lat2 = Double.parseDouble(items[0]);
    	double long2 = Double.parseDouble(items[1]);
    	
    	return getGeoDistance(lat1, long1, lat2, long2) ;
    }
    
    /**
     * geo location distance by Haversine formula
     * @param lat1
     * @param long1
     * @param lat2
     * @param long2
     * @return distance in km
     */
    public static int getGeoDistance(double lat1, double long1, double lat2, double long2) {
        double latDistance = Math.toRadians(lat1 - lat2);
        double longDistance = Math.toRadians(long1 - long2);

        double a = (Math.sin(latDistance / 2) * Math.sin(latDistance / 2)) + (Math.cos(Math.toRadians(lat1))) *
                        (Math.cos(Math.toRadians(lat2))) * (Math.sin(longDistance / 2)) * (Math.sin(longDistance / 2));
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return (int) (Math.round(AVERAGE_RADIUS_OF_EARTH * c));

    }    
    
    /**
     * @param conf
     * @return
     * @throws Exception
     */
    public static SingleTypeSchema getSameTypeSchema(Configuration conf) throws Exception  {
		//schema
        String filePath = conf.get("same.schema.file.path");
        FileSystem dfs = FileSystem.get(conf);
        Path src = new Path(filePath);
        FSDataInputStream fs = dfs.open(src);
        ObjectMapper mapper = new ObjectMapper();
        SingleTypeSchema schema = mapper.readValue(fs, SingleTypeSchema.class);
    	return schema;
    }
    
    
}
