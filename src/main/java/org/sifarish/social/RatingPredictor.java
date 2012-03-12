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
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.chombo.util.TextInt;
import org.chombo.util.TextPair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.sifarish.feature.TextIntPair;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Predicts rating for an user and item. 2nd MR for slope one recommender
 * @author pranab
 *
 */
public class RatingPredictor extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Rating predictor  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(RatingPredictor.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(RatingPredictor.PredictionMapper.class);
        job.setReducerClass(RatingPredictor.PredictorReducer.class);
        
        job.setMapOutputKeyClass(TextInt.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        Utility.setConfiguration(job.getConfiguration());
        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    public static class PredictionMapper extends Mapper<LongWritable, Text, TextInt, Tuple> {
    	private String fieldDelim;
    	private String subFieldDelim;
    	private boolean isRatingFileSplit;
    	private TextInt keyOut = new TextInt();
    	private Tuple valOut = new Tuple();
    	private String[] ratings;
    	private Integer one = 1;
    	private Integer zero = 0;
    	
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	subFieldDelim = context.getConfiguration().get("field.delim", ":");
        	String ratingFilePrefix = context.getConfiguration().get("rating.file.prefix", "rating");
        	isRatingFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(ratingFilePrefix);
        }    
    	
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	String[] items = value.toString().split(fieldDelim);
        	if (isRatingFileSplit) {
        		//user rating
        		String userID = items[0];
               	for (int i = 1; i < items.length; ++i) {
               		valOut.initialize();
            		ratings = items[i].split(subFieldDelim);
            		keyOut.set(ratings[0], 1);
            		valOut.add(userID,  new Integer(ratings[1]), one);
       	   			context.write(keyOut, valOut);
               	}
        	} else {
        		//rating difference
        		keyOut.set(items[0], 0);
        		valOut.add(items[1], new Integer(items[2]), new Integer(items[3]), zero);
   	   			context.write(keyOut, valOut);

   	   			keyOut.set(items[1], 0);
        		valOut.add(items[0], new Integer("-" + items[2]), new Integer(items[3]));
   	   			context.write(keyOut, valOut);
        	}
        }
    }    

    public static class PredictorReducer extends Reducer<TextInt, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valueOut = new Text();
    	private List<Tuple> avRatingDiffs = new ArrayList<Tuple>();
    	
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        } 	
        
        protected void reduce(TextInt  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	avRatingDiffs.clear();
           	for(Tuple value : values) {
           		if ( ((Integer)value.get(value.getSize()-1)) == 0) {
           			avRatingDiffs.add(value);
           		} else {
           			String userID = value.getString(0);
           			int rating = value.getInt(1);
           			
           			for (Tuple  ratingDiffTup : avRatingDiffs) {
           				String itemID = ratingDiffTup.getString(0);
           				int ratingDiff = ratingDiffTup.getInt(1);
           				int weight = ratingDiffTup.getInt(2);
           				
           				valueOut.set(userID + fieldDelim + itemID + fieldDelim + (rating+ratingDiff) + fieldDelim + weight);
           		   		context.write(NullWritable.get(), valueOut);
           			}
           		}
           	}        	
        }
    }
    
}
