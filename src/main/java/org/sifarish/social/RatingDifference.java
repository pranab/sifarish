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
import org.chombo.util.TextPair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Average rating difference between 2 items. First MR for slope one recommender
 * @author pranab
 *
 */
public class RatingDifference extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Rating difference MR";
        job.setJobName(jobName);
        
        job.setJarByClass(RatingDifference.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(RatingDifference.DiffMapper.class);
        job.setReducerClass(RatingDifference.DiffReducer.class);
        
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

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
    public static class DiffMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    	private String fieldDelim;
    	private String subFieldDelim;
    	private TextPair keyOut = new TextPair();
    	private IntWritable valOut = new IntWritable();
    	
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	subFieldDelim = context.getConfiguration().get("field.delim", ":");
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	String[] items = value.toString().split(fieldDelim);
        	String[] ratings = null;
        	for (int i = 1; i < items.length; ++i) {
        		ratings = items[i].split(subFieldDelim);
        		String itemOne = ratings[0];
        		int ratingOne = Integer.parseInt(ratings[1]);
        		for (int j = i+1; j <  items.length; ++j) {
            		ratings = items[i].split(subFieldDelim);
            		String itemTwo = ratings[0];
            		int  ratingTwo = Integer.parseInt(ratings[1]);
            		if (itemOne.compareTo(itemTwo ) < 0) {
            			keyOut.set(itemOne, itemTwo);
            			valOut.set(ratingOne - ratingTwo);
            		} else {
            			keyOut.set(itemTwo, itemOne);
            			valOut.set(ratingTwo - ratingOne);
            		}
       	   			context.write(keyOut, valOut);
        		}
        	}
        }
    }
    
    /**
     * @author pranab
     *
     */
    public static class DiffReducer extends Reducer<TextPair, IntWritable, NullWritable, Text> {
    	private String fieldDelim;
    	private int sum ;
    	private int count;
    	private int avRating;
    	private Text valueOut = new Text();
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        } 	
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(TextPair  key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        	sum = count = 0;
        	for(IntWritable value : values) {
        		sum += value.get();
        		++count;
        	}
        	avRating = sum / count;
        	valueOut.set(key.toString() + fieldDelim +  avRating + fieldDelim + count);
	   		context.write(NullWritable.get(), valueOut);
          }    	
    }        	

}
