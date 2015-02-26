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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;


/**
 * Blends implict rating based on click stream, explicit rating and rating from
 * CRM or customer service system to derive an aggregated rating based on weighted average
 * @author pranab
 *
 */
public class RatingBlender extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Rating blender MR";
        job.setJobName(jobName);
        
        job.setJarByClass(RatingBlender.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(RatingBlender.RatingBlenderlMapper.class);
        job.setReducerClass(RatingBlender.RatingBlenderReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("rab.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    /**
     * @author pranab
     *
     */
    public static class RatingBlenderlMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
    	private String fieldDelim;
    	private Tuple keyOut = new Tuple();
    	private Tuple valOut = new Tuple();
    	private boolean isExplicitRatingFileSplit;
    	private boolean isCustSvcRatingFileSplit;
    	private String userID;
    	private String itemID;
    	private int rating ;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	String explicitRatingFilePrefix = context.getConfiguration().get("explicit.rating.file.prefix", "expl");
        	String custSvcRatingFilePrefix = context.getConfiguration().get("custsvc.rating.file.prefix", "cust");
        	
        	String splitName = ((FileSplit)context.getInputSplit()).getPath().getName();
        	isExplicitRatingFileSplit = splitName.startsWith(explicitRatingFilePrefix);
        	isCustSvcRatingFileSplit = splitName.startsWith(custSvcRatingFilePrefix);
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	String[] items = value.toString().split(fieldDelim);
           	userID = items[0];
           	itemID = items[1];
           	rating = Integer.parseInt(items[2]);
           	
           	keyOut.initialize();
           	valOut.initialize();
           	if (isExplicitRatingFileSplit) {
           		setKeyValue(1);
           	} else if (isCustSvcRatingFileSplit) {
           		setKeyValue(2);
           	} else {
           		setKeyValue(0);
           	}
           	context.write(keyOut, valOut);
        }  
        
        /**
         * Sets key and value
         * @param secodaryKey
         */
        private void setKeyValue(int secodaryKey) {
       		//userID, item ID
       		keyOut.add(userID, itemID, secodaryKey);

       		//rating
       		valOut.add(secodaryKey, rating);
        }
    }
    
    /**
     * @author pranab
     *
     */
    public static class RatingBlenderReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private Map<Integer, int[]> ratingWeights = new HashMap<Integer, int[]>();
    	private String userID;
    	private String itemID;
    	private int rating;
    	private int ratingSum;
    	private int[] ratingSource = new int[3];
    	private int[] ratingWeightsByCase;
    	private static final int IMPLICIT = 1;
    	private static final int IMPLICIT_EXPLICIT = 2;
    	private static final int IMPLICIT_CUSTSVC = 3;
    	private static final int IMPLICIT_EXPLICIT_CUSTSVC = 4;
        private  final int  MAX_WEIGHT = 100;
        
    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        	int[] ratingWeightList = Utility.intArrayFromString(config.get("biz.goal.weights"),fieldDelim );
        	
        	//all sources
        	ratingWeights.put(IMPLICIT_EXPLICIT_CUSTSVC, ratingWeightList);
        	
        	//implicit and explicit
        	int[] modRatingWeights = new int[3];
        	modRatingWeights[0] = (ratingWeightList[0] * MAX_WEIGHT) / (ratingWeightList[0] + ratingWeightList[1]);
        	modRatingWeights[1] = (ratingWeightList[1] * MAX_WEIGHT) / (ratingWeightList[0] + ratingWeightList[1]);
        	modRatingWeights[2] = 0;
        	ratingWeights.put(IMPLICIT_EXPLICIT, modRatingWeights);
        	
        	//implicit and cust svc
        	modRatingWeights[0] = (ratingWeightList[0] * MAX_WEIGHT) / (ratingWeightList[0] + ratingWeightList[2]);
        	modRatingWeights[2] = (ratingWeightList[2] * MAX_WEIGHT) / (ratingWeightList[0] + ratingWeightList[2]);
        	modRatingWeights[1] = 0;
        	ratingWeights.put(IMPLICIT_CUSTSVC, modRatingWeights);
        	
        	//implicit only
        	modRatingWeights[0] = 100;
        	modRatingWeights[1] = 0;
        	modRatingWeights[2] = 0;
        	ratingWeights.put(IMPLICIT, modRatingWeights);
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	userID = key.getString(0);
        	itemID = key.getString(1);

        	for (int i = 0; i < 3; ++i) {
        		ratingSource[i] = 0;
        	}
        	for(Tuple value : values) {
        		ratingSource[value.getInt(0)] = value.getInt(1);
        	}
        	
        	
        	//rating weights depending on the case
        	if (ratingSource[1] == 0 && ratingSource[2] == 0) {
        		//implicit only
        		ratingWeightsByCase = ratingWeights.get(IMPLICIT);
        	} else if (ratingSource[1] > 0 && ratingSource[2] == 0) {
        		//implicit and explicit only
        		ratingWeightsByCase = ratingWeights.get(IMPLICIT_EXPLICIT);
        	} else if (ratingSource[1] == 0 && ratingSource[2] > 0) {
        		//implicit and cust svc  only
        		ratingWeightsByCase = ratingWeights.get(IMPLICIT_CUSTSVC);
        	} else {
        		//all
        		ratingWeightsByCase = ratingWeights.get(IMPLICIT_EXPLICIT_CUSTSVC);
        	}
        	
        	//aggregate rating
        	ratingSum = 0;
        	for (int i = 0; i < 3; ++i) {
        		ratingSum += ratingSource[i] * ratingWeightsByCase[i];
        	}
        	rating = ratingSum / MAX_WEIGHT;
        	
        	valOut.set(userID + fieldDelim + itemID + fieldDelim + rating);
			context.write(NullWritable.get(), valOut);
        }
        
    }

}
