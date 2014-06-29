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

package org.sifarish.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Item engaement distribution per user
 * @author pranab
 *
 */
public class ItemEngagementDistr extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Item implicit rating distribution per user  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(ItemEngagementDistr.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(ItemEngagementDistr.EngagementDistrMapper.class);
        job.setReducerClass(ItemEngagementDistr.EngagementDistrReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
		
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

    /**
     * @author pranab
     *
     */
    public static class EngagementDistrMapper extends Mapper<LongWritable, Text, Text, Tuple> {
        private String fieldDelimRegex;
        private Text outKey = new Text();
        private Tuple outVal = new Tuple();
  
	    /* (non-Javadoc)
	     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
	    }
	    /* (non-Javadoc)
	     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    protected void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
        	String[] items  =  value.toString().split(fieldDelimRegex);
        	
        	//emit userID as key and itemID,  rating as value
        	outKey.set(items[0]);
        	outVal.initialize();
        	outVal.add(items[1],  Integer.parseInt(items[2]));
	   		context.write(outKey, outVal);
	    }    
    }
	
    /**
     * @author pranab
     *
     */
    public static class EngagementDistrReducer extends Reducer<Text, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private StringBuilder stBld = new StringBuilder();
    	private Map<String, Integer> itemRatings = new HashMap<String, Integer>();
    	private int sum;
    	private int engagementDistrScale;
    	private int  distr;
    	private int rating;
    	private String userID;
    	
    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        	engagementDistrScale = config.getInt("engagement.distr.scale",  1000);
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Text  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	itemRatings.clear();
       		sum = 0;
           	for(Tuple value : values) {
           		rating = value.getInt(1);
           		itemRatings.put(value.getString(0), rating);
           		sum += rating;
           	}
           	
           	//all items
           	userID = key.toString();
           	for (String itemID :  itemRatings.keySet()) {
        		stBld.delete(0,  stBld.length());
           		stBld.append(userID).append(fieldDelim).append(itemID);

           		rating = itemRatings.get(itemID);
           		distr = (rating * engagementDistrScale) /  sum;

           		stBld.append(fieldDelim).append(rating);
          		stBld.append(fieldDelim).append(distr);
          		
          		//userID,  itemID, rating, rating distr
               	valOut.set(stBld.toString());
        		context.write(NullWritable.get(), valOut);
           	}
        }
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ItemEngagementDistr(), args);
        System.exit(exitCode);
    }
   
}
