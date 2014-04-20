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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.chombo.util.Pair;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Estimates implicit rating based on user engagement bahavior with items 
 * @author pranab
 *
 */
public class ImplicitRatingEstimator   extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Implicit rating estimator MR";
        job.setJobName(jobName);
        
        job.setJarByClass(ImplicitRatingEstimator.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ImplicitRatingEstimator.RatingEstimatorMapper.class);
        job.setReducerClass(ImplicitRatingEstimator.RatingEstimatorReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }

    /**
     * @author pranab
     *
     */
    public static class RatingEstimatorMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
    	private String fieldDelim;
    	private Tuple keyOut = new Tuple();
    	private Tuple  valOut = new Tuple();
    	private String userID;
    	private int  eventType = 0;
    	private long timeStamp;
    	private static final int USER_ID_ORD = 0;
    	private static final int ITEM_ID_ORD = 2;
    	private static final int EVT_ORD = 3;
    	private static final int TS_ORD = 4;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim.regex", ",");
        }    
   
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	String[] items = value.toString().split(fieldDelim);
           	userID = items[USER_ID_ORD];
           	if (!userID.isEmpty()) {
           		//process only logged in sessions
	           	eventType = Integer.parseInt(items[EVT_ORD]);
	           	timeStamp = Long.parseLong(items[TS_ORD]);
	           	
	           	//user ID, item ID, event
	           	keyOut.initialize();
	           	keyOut.add(items[USER_ID_ORD], items[ITEM_ID_ORD], eventType);
	           	
	           	valOut.initialize();
	           	valOut.add(eventType, timeStamp);
	           	context.write(keyOut, valOut);
				context.getCounter("ImplicitRating", "Processed record").increment(1);
           	}
        }       
    }    
    
    /**
     * @author pranab
     *
     */
    public static class RatingEstimatorReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private int rating;
    	private EngagementToPreferenceMapper ratingMapper;
    	private int mostEngagingEventType ;
    	private int count;
    	private int eventType;
    	private long timeStamp;
    	private long latestTimeStamp;
    	private  boolean outputDetail;
    	private StringBuilder stBld = new StringBuilder();
    	private List<Pair<Integer,Long>> events = new ArrayList<Pair<Integer,Long>>();
    	//private List<Pair<Integer,Long>> filteredEvents = new ArrayList<Pair<Integer,Long>>();
    	private Map<Integer,Integer> negativeEventCounts = new HashMap<Integer,Integer>();
    	private Map<Integer,Integer> eventCounts = new HashMap<Integer,Integer>();
    	private int thisRating;
    	
    	
    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        	InputStream fs  = Utility.getFileStream( config, "rating.mapper.config.path"); 
            ObjectMapper mapper = new ObjectMapper();
            ratingMapper = mapper.readValue(fs, EngagementToPreferenceMapper.class);
            outputDetail = config.getBoolean("rating.estimator.output.detail", false);
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	if (stBld.length() > 0) {
        		stBld.delete(0,  stBld.length() -1);
        	}
        	
        	//separate negative and positive events
        	events.clear();
        	//filteredEvents.clear();
        	negativeEventCounts.clear();
        	eventCounts.clear();
        	for(Tuple value : values) {
        		eventType = value.getInt(0);
        		timeStamp = value.getLong(1);
        		if (eventType < 0) {
        			Integer negCount = negativeEventCounts.get(eventType);
        			negCount = null == negCount?  1 : negCount + 1;
        			negativeEventCounts.put(eventType, negCount);
        		} else {
        			events.add(new Pair<Integer,Long>(eventType, timeStamp));
        		}
        	}
        	
        	//nullify those positive events that have negatives
        	latestTimeStamp = 0;
        	for(Pair<Integer,Long> event : events) {
       			//latest time stamp in the vent sequence 
        		timeStamp = event.getRight();
        		eventType = event.getLeft();
    			if(timeStamp > latestTimeStamp) {
    				latestTimeStamp = timeStamp;
    			}

    			Integer negCount =  negativeEventCounts.get(-eventType);
        		if (null != negCount && negCount > 0) {
        			negativeEventCounts.put(-eventType, negCount - 1);
    				context.getCounter("ImplicitRating", "Num negative events").increment(1);
        		} else {
        			//filteredEvents.add(event);
        			Integer posCount = eventCounts.get(eventType);
        			posCount = null == posCount?  1 : posCount + 1;
        			eventCounts.put(eventType, posCount);
        		}
        	}
        	
        	//find most engaging event and corresponding rating
        	mostEngagingEventType =  0;
        	rating = 0;
        	for (int thisEvent : eventCounts.keySet()) {
            	thisRating =ratingMapper.scoreForEvent(thisEvent, eventCounts.get(thisEvent));
        		if(thisRating > rating) {
        			rating = thisRating;
        			mostEngagingEventType = thisEvent;
        		}
        	}
        	
        	
        	//find most engaging along with count
        	/*
        	boolean first = true;
        	count = 0;
        	latestTimeStamp = 0;
        	for(Pair<Integer,Long> event : filteredEvents) {
        		eventType = event.getLeft();
        		timeStamp = event.getRight();
        		if (first) {
        			mostEngagingEventType = eventType;
        			++count;
        			first = false;
        		} else {
        			//all occurences of the first event type
        			if (eventType  == mostEngagingEventType) {
        				++count;
        			} 
        		}
       			//latest time stamp in the vent sequence 
    			if(timeStamp > latestTimeStamp) {
    				latestTimeStamp = timeStamp;
    			}
        	}
        	*/
        	
        	//rating =ratingMapper.scoreForEvent(mostEngagingEventType, count);
        	stBld.append(key.getString(0)).append(fieldDelim).append(key.getString(1)).
        		append(fieldDelim).append(rating).append(fieldDelim).append(latestTimeStamp);
        	if(outputDetail) {
        		stBld.append(fieldDelim).append(mostEngagingEventType).append(fieldDelim).append(count);
        	}
        	
        	valOut.set(stBld.toString());
			context.write(NullWritable.get(), valOut);
        }       
        
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ImplicitRatingEstimator(), args);
        System.exit(exitCode);
    }
   
}
