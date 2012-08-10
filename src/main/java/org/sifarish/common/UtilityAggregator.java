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
import org.chombo.util.IntPair;
import org.chombo.util.TextPair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class UtilityAggregator extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Rating aggregator MR";
        job.setJobName(jobName);
        
        job.setJarByClass(UtilityAggregator.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(UtilityAggregator.AggregateMapper.class);
        job.setReducerClass(UtilityAggregator.AggregateReducer.class);
        
        job.setMapOutputKeyClass(TextPair.class);
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
    public static class AggregateMapper extends Mapper<LongWritable, Text, TextPair, Tuple> {
    	private String fieldDelim;
    	private TextPair keyOut = new TextPair();
    	private Tuple valOut = new Tuple();
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	String[] items = value.toString().split(fieldDelim);
           	keyOut.set(items[0], items[1]);   	
           	valOut.initialize();
           	valOut.add(new Integer(items[2]), new Integer(items[3]),  new Integer(items[4]));
	   		context.write(keyOut, valOut);
        }   
    }
    
    /**
     * @author pranab
     *
     */
    public static class AggregateReducer extends Reducer<TextPair, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private int sum ;
    	private int sumWt;
    	private int avRating;
    	private Text valueOut = new Text();
    	private boolean weightedAverage;
    	private boolean ratingAggregatorAverage;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	weightedAverage = context.getConfiguration().getBoolean("weighted.average", true);
        	ratingAggregatorAverage = context.getConfiguration().getBoolean("rating.aggregator.average", true);
        } 	
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(TextPair  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
			if (ratingAggregatorAverage) {
				//average
				sum = sumWt = 0;
				for(Tuple value : values) {
					if (weightedAverage) {
						sum += value.getInt(0) * value.getInt(1);
						sumWt += value.getInt(1);
					} else {
						sum += value.getInt(0);
						++sumWt;
					}
				}
				avRating = sum / sumWt;
			} else {
				//median
			}
			
        	valueOut.set(key.getFirst() + fieldDelim + key.getSecond() + fieldDelim + avRating);
	   		context.write(NullWritable.get(), valueOut);
        }
    }
}
