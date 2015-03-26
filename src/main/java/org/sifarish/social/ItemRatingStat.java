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

package org.sifarish.social;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.BigTupleList;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.sifarish.common.ItemDynamicAttributeSimilarity;

/**
 * Calculates per item rating statistics
 * @author pranab
 *
 */
public class ItemRatingStat extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Rating statistics  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(ItemRatingStat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ItemRatingStat.StatMapper.class);
        job.setReducerClass(ItemRatingStat.StatReducer.class);
       job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("irs.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    /**
     * @author pranab
     *
     */
    public static class StatMapper extends Mapper<LongWritable, Text,  Text, Tuple> {
    	private String fieldDelim;
    	private String subFieldDelim;
    	private String itemID;
    	private int rating;
    	private int ratingSum;
    	private int ratingSquareSum;
    	private int ratingMean;
    	private int ratingStdDev;
    	private Text keyOut = new Text();
    	private Tuple  valOut = new Tuple();
    	private int  count;
    	private List<Integer> ratings = new ArrayList<Integer>();
    	private int ratingMedian;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	subFieldDelim = context.getConfiguration().get("subfield.delim", ":");
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	String[] items = value.toString().split(fieldDelim);
        	itemID = items[0];
        	
        	ratingSum = 0;
        	ratingSquareSum = 0;
        	ratings.clear();
        	for (int i = 1; i < items.length; ++ i) {
        		rating = ( Integer.parseInt(items[i].split(subFieldDelim)[1]));
        		ratingSum += rating;
        		ratingSquareSum += (rating * rating);
        		ratings.add(rating);
        	}
        	//mean
        	count = items.length - 1;
        	ratingMean = ratingSum / count;
        	
        	//median
			if (count > 1) {
				Collections.sort(ratings);
				if (count % 2 == 1) {
					//odd
					ratingMedian = ratings.get(count / 2);
				} else {
					//even
					ratingMedian =  (ratings.get(count / 2 - 1) + ratings.get(count / 2) )  / 2 ;
				}
			} else {
				ratingMedian = ratings.get(0);
			}
       	
        	//std dev
        	int var = ratingSquareSum /  count -  ratingMean * ratingMean;
        	ratingStdDev = (int)Math.sqrt(var);
			
        	keyOut.set(itemID);
        	valOut.initialize();
        	valOut.add(ratingMean, ratingMedian, ratingStdDev,  count);
   		   	context.write(keyOut, valOut);
        }   
    }
 
    /**
     * @author pranab
     *
     */
    public static class StatReducer extends Reducer<Text, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private int maxCount;
    	private int maxRatingMean;
    	private int maxRatingMedian;
    	private int maxRatingStdDev;
    	private int thisCount;
    	private int thisRatingMean;
    	private int thisRatingMedian;
    	private int thisRatingStdDev;
    	private int ratingScale;
    	private BigTupleList tupleList;
    	private Tuple tupleToWrite;
    	private int statsScale;
       	private StringBuilder stBld = new StringBuilder();
       	private boolean normalizedOutput;
       	private static final int MEAN_ORD = 0;
       	private static final int MEDIAN_ORD = 1;
       	private static final int STD_DEV_ORD = 2;
       	private static final int COUNT_ORD = 3;
       	
        private static final Logger LOG = Logger.getLogger(ItemRatingStat.StatReducer.class);
      	
       	
       	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             	System.out.println("in debug mode");
            }
        	fieldDelim = config.get("field.delim", ",");
        	ratingScale = context.getConfiguration().getInt("rating.scale", 100);
        	normalizedOutput = config.getBoolean("normalized.output", false);
    		if (normalizedOutput) {
	        	int maxInMemory  = config.getInt("tuple.list.max.in.memory", 1000);
	        	String spillFilePath = config.get("tuple.list.spill.dir", "/tmp/bigTupleList");
	        	tupleList = new BigTupleList(maxInMemory, spillFilePath);
	        	tupleList.open(BigTupleList.Mode.Write);
	        	statsScale = context.getConfiguration().getInt("stats.scale", 100);
	        	maxCount = 0;
	        	maxRatingMean = 0;
	        	maxRatingMedian = 0;
	        	maxRatingStdDev = 0;
    		}
        }

	   	protected void cleanup(Context context)  
	   			throws IOException, InterruptedException {
	   		if (normalizedOutput) {
	   			LOG.debug("maxRatingMean:" + maxRatingMean + "maxRatingMedian:" + maxRatingMedian + 
	   					" maxRatingStdDev:" + maxRatingStdDev + " maxCount:" + maxCount);
	        	tupleList.close();
	        	tupleList.open(BigTupleList.Mode.Read);
	        	Tuple value = null;
	        	while ((value = tupleList.read()) != null) {
	        		thisCount = value.getInt(COUNT_ORD + 1) * statsScale / maxCount;
	        		thisRatingMean = value.getInt(MEAN_ORD + 1) * statsScale / maxRatingMean ;
	        		thisRatingMedian = value.getInt(MEDIAN_ORD + 1) * statsScale / maxRatingMedian ;
	        		thisRatingStdDev =  value.getInt(STD_DEV_ORD + 1) * statsScale /  maxRatingStdDev;
	    			stBld.delete(0, stBld.length());
	    			stBld.append(value.getString(0)).append(fieldDelim).append(thisRatingMean).append(fieldDelim).
	    				append(thisRatingMedian).append(fieldDelim).append(thisRatingStdDev).append(fieldDelim).append(thisCount);
	    			
	        		valOut.set(stBld.toString());
	    			context.write(NullWritable.get(), valOut);
	        	}
	           	tupleList.close();
	   		}
	   	}
	   	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Text  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	for(Tuple value : values) {
        		if (normalizedOutput) {
	        		thisCount = value.getInt(COUNT_ORD);
	        		thisRatingMean = value.getInt(MEAN_ORD);
	        		thisRatingMedian = value.getInt(MEDIAN_ORD);
	        		thisRatingStdDev =  value.getInt(STD_DEV_ORD);
	        		if (thisCount > maxCount) {
	        			maxCount = thisCount;
	        		}
	        		if (thisRatingMean > maxRatingMean){
	        			maxRatingMean = thisRatingMean;
	        		}
	        		if (thisRatingMedian > maxRatingMedian){
	        			maxRatingMedian = thisRatingMedian;
	        		}
	        		if (thisRatingStdDev > maxRatingStdDev){
	        			maxRatingStdDev = thisRatingStdDev; 
	        		}
	        		if (null == tupleToWrite) {
	        			tupleToWrite = value.createClone();
	        		} else {
	        			tupleToWrite = value.createClone(tupleToWrite);
	        		}
	        		tupleToWrite.prepend(key.toString());
	       			tupleToWrite = tupleList.write(tupleToWrite);
        		} else {
        			valOut.set(key.toString() + fieldDelim + value.toString());
        			context.write(NullWritable.get(), valOut);
        		}
        	}
        }        
    }      
  
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ItemRatingStat(), args);
        System.exit(exitCode);
    }
    
}
