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
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Reorders recommender ranks based user explicit positive feedback on items
 * @author pranab
 *
 */
public class PositiveFeedbackBasedRankReorderer  extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Positive feedback based rank reordering MR";
        job.setJobName(jobName);
        
        job.setJarByClass(PositiveFeedbackBasedRankReorderer.class);
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PositiveFeedbackBasedRankReorderer.PositiveFeedbackMapper.class);
        job.setReducerClass(PositiveFeedbackBasedRankReorderer.PositiveFeedbackReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        Utility.setConfiguration(job.getConfiguration());
        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }

    /**
     * @author pranab
     *
     */
    public static class PositiveFeedbackMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
    	private String fieldDelimRegex;
    	private Tuple keyOut = new Tuple();
    	private Tuple valOut = new Tuple();
    	private boolean isActualRatingFileSplit;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	String actualRatingFilePrefix = config.get("actual.rating.file.prefix", "actual");
        	isActualRatingFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(actualRatingFilePrefix);
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	String[] items = value.toString().split(fieldDelimRegex);
           	keyOut.initialize();
           	valOut.initialize();
       		keyOut.add(items[0], items[1]);
           	if (isActualRatingFileSplit) {
           		valOut.add(0,  Integer.parseInt(items[2]));
           	} else {
           		valOut.add(1,  Integer.parseInt(items[2]));
           	}
           	context.write(keyOut, valOut);
        }
    }

    /**
     * @author pranab
     *
     */
    public static class PositiveFeedbackReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
 		private StringBuilder stBld =  new StringBuilder();
 		private Integer actualRating;
 		private Integer predictedRating;
 		private Integer finalRating;
 		private String ratingAggrStrategy;
 		private int actualRatingWt;
 		private int maxRating;
 		
    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        	ratingAggrStrategy = config.get("rating.aggr.strategy", "max");
        	if (ratingAggrStrategy.equals("weightedAverage")) {
        		actualRatingWt = config.getInt("actual.rating.weight", 50);
        	}
        	maxRating =  config.getInt("max.rating", 100);
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
       		stBld.delete(0, stBld.length());
       		actualRating = null;
       		predictedRating = null;
       		finalRating = null;
        	for(Tuple value : values) {
        		if (value.getInt(0) == 0) {
        			actualRating = value.getInt(1);
        		} else {
        			predictedRating = value.getInt(1);
        		}
        	}
        	
        	//final rating 
            if (null != actualRating && actualRating == maxRating) {
        		context.getCounter("Rating", "Actual max and converted").increment(1);
            } else if (null == actualRating) {
            	finalRating = predictedRating;
	    		context.getCounter("Rating", "Predicted").increment(1);
            } else if(null == predictedRating) {
            	finalRating = actualRating;
            	context.getCounter("Rating", "Actual").increment(1);
            } else {
	    		context.getCounter("Rating", "Both").increment(1);
            	if (ratingAggrStrategy.equals("max")) {
                	finalRating = Math.max(actualRating, predictedRating);
            	} else if (ratingAggrStrategy.equals("weightedAverage")) {
                	finalRating =( actualRatingWt * actualRating + (100 - actualRatingWt) * predictedRating) / 100;
            	} else {
            		throw new IllegalArgumentException("Invalid rating aggregation strategy");
            	}
            }
            
    		//userID, itemID, rating
            if (null != finalRating) {
            	stBld.append(key.get(0)).append(fieldDelim).append(key.get(1)).append(fieldDelim).append(finalRating);
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
        int exitCode = ToolRunner.run(new PositiveFeedbackBasedRankReorderer(), args);
        System.exit(exitCode);
    }
   
}
