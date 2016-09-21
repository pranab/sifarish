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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.TextInt;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Predicts rating for an user and item. based on another item the user has rated and the 
 * correlation between the items
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
 
        job.setGroupingComparatorClass(ItemIdGroupComprator.class);
        job.setPartitionerClass(ItemIdPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("rap.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    /**
     * @author pranab
     *
     */
    public static class PredictionMapper extends Mapper<LongWritable, Text, TextInt, Tuple> {
    	private String fieldDelim;
    	private String subFieldDelim;
    	private boolean isRatingFileSplit;
    	private TextInt keyOut = new TextInt();
    	private Tuple valOut = new Tuple();
    	private String[] ratings;
    	private Integer one = 1;
    	private Integer zero = 0;
    	private boolean linearCorrelation;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	subFieldDelim = context.getConfiguration().get("field.delim", ":");
        	String ratingFilePrefix = context.getConfiguration().get("rap.rating.file.prefix", "rating");
        	isRatingFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(ratingFilePrefix);
        	linearCorrelation = context.getConfiguration().getBoolean("rap.correlation.linear", true);
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	String[] items = value.toString().split(fieldDelim);
        	if (isRatingFileSplit) {
        		//user rating
        		String itemID = items[0];
               	for (int i = 1; i < items.length; ++i) {
               		valOut.initialize();
            		ratings = items[i].split(subFieldDelim);
            		keyOut.set(itemID, 1);
            		valOut.add(ratings[0],  new Integer(ratings[1]), one);
       	   			context.write(keyOut, valOut);
               	}
        	} else {
        		//rating correlation
        		keyOut.set(items[0], 0);
        		valOut.add(items[1], new Integer(items[2]), new Integer(items[3]), zero);
   	   			context.write(keyOut, valOut);

   	   			keyOut.set(items[1], 0);
   	   			if (linearCorrelation) {
   	   				valOut.add(items[0], new Integer( items[2]), new Integer(items[3]), zero);
   	   			} else {
   	   				valOut.add(items[0], new Integer("-" + items[2]), new Integer(items[3]), zero);
   	   			}
   	   			context.write(keyOut, valOut);
        	}
        }
    }    

    /**
     * @author pranab
     *
     */
    public static class PredictorReducer extends Reducer<TextInt, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valueOut = new Text();
    	private List<Tuple> avRatingDiffs = new ArrayList<Tuple>();
    	private boolean linearCorrelation;
    	private int correlationScale;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	linearCorrelation = context.getConfiguration().getBoolean("rap.correlation.linear", true);
        	correlationScale = context.getConfiguration().getInt("rap.correlation.linear.scale", 1000);
        } 	
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(TextInt  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	avRatingDiffs.clear();
           	for(Tuple value : values) {
           		if ( ((Integer)value.get(value.getSize()-1)) == 0) {
           			avRatingDiffs.add(value);
           		} else {
           			if (!avRatingDiffs.isEmpty()) {
	           			String userID = value.getString(0);
	           			int rating = value.getInt(1);
	           			
	           			for (Tuple  ratingDiffTup : avRatingDiffs) {
	           				String itemID = ratingDiffTup.getString(0);
	           				int ratingCorr = ratingDiffTup.getInt(1);
	           				int weight = ratingDiffTup.getInt(2);
	           				
	           				int predRating = linearCorrelation? (rating * ratingCorr) / correlationScale : rating + ratingCorr;
	           				valueOut.set(userID + fieldDelim + itemID + fieldDelim + predRating + fieldDelim + weight);
	           		   		context.write(NullWritable.get(), valueOut);
	           			}
           			}
           		}
           	}        	
        }
    }
    
    /**
     * @author pranab
     *
     */
    public static class ItemIdPartitioner extends Partitioner<TextInt, Tuple> {
	     @Override
	     public int getPartition(TextInt key, Tuple value, int numPartitions) {
	    	 //consider only base part of  key
		     return key.baseHashCode()% numPartitions;
	     }
   
   }

    /**
     * @author pranab
     *
     */
    public static class ItemIdGroupComprator extends WritableComparator {
    	protected ItemIdGroupComprator() {
    		super(TextInt.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		//consider only the base part of the key
    		TextInt t1 = ((TextInt)w1);
    		TextInt t2 = ((TextInt)w2);
    		return t1.baseCompareTo(t2);
    	}
     }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RatingPredictor(), args);
        System.exit(exitCode);
    }
    
}
