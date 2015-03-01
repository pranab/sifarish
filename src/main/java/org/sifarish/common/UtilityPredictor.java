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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import  org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.TextInt;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Predicts rating for an user and item. based on another item the user has rated and the 
 * correlation between the items. This is the second MR to run after rating correlations are available
 * @author pranab
 *
 */
public class UtilityPredictor extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Rating predictor  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(UtilityPredictor.class);
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(UtilityPredictor.PredictionMapper.class);
        job.setReducerClass(UtilityPredictor.PredictorReducer.class);
        
        job.setMapOutputKeyClass(TextInt.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(ItemIdGroupComprator.class);
        job.setPartitionerClass(ItemIdPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("utp.num.reducer", -1);
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
    	private Integer two = 2;
    	private Integer one = 1;
    	private Integer zero = 0;
    	private boolean linearCorrelation;
    	private boolean isRatingStatFileSplit;
    	private long  ratingTimeCutoff;
    	private  long timeStamp;
    	private int minInputRating;
    	private  int inputRating;
    	private int minCorrelation;
    	private int correlation;
    	private int  correlationLength;
    	private static final int STD_DEV_ORD = 3;
    	
        private static final Logger LOG = Logger.getLogger(UtilityPredictor.PredictionMapper.class);

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
            if (conf.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             	System.out.println("in debug mode");
            }

            fieldDelim = conf.get("field.delim", ",");
        	subFieldDelim = conf.get("sub.field.delim", ":");
        	String ratingFilePrefix = conf.get("rating.file.prefix", "rating");
        	isRatingFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(ratingFilePrefix);
        	String ratingStatFilePrefix = conf.get("rating.stat.file.prefix", "stat");
        	isRatingStatFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(ratingStatFilePrefix);
        	
        	linearCorrelation = conf.getBoolean("correlation.linear", true);
        	int ratingTimeWindow = conf.getInt("rating.time.window.hour",  -1);
        	ratingTimeCutoff = ratingTimeWindow > 0 ?  System.currentTimeMillis() / 1000 - ratingTimeWindow * 60L * 60L : -1;
        	
        	minInputRating = conf.getInt("min.input.rating",  -1);
        	minCorrelation = conf.getInt("min.correlation",  -1);
        	LOG.info("isRatingFileSplit:" + isRatingFileSplit);
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	String[] items = value.toString().split(fieldDelim);
    		String itemID = items[0];
        	if (isRatingFileSplit) {
        		//user rating
				//context.getCounter("Record type count", "Rating").increment(1);

        		boolean toInclude = true;
               	for (int i = 1; i < items.length; ++i) {
               		//all user ratings for this item
            		ratings = items[i].split(subFieldDelim);
            		
            		//time sensitive recommendation
            		 toInclude = true; 
            		 if (ratingTimeCutoff > 0) {
            			 timeStamp = Long.parseLong(ratings[2]);
            			 toInclude = timeStamp > ratingTimeCutoff;
            		 }
            
            		 //check for min input rating threshold
           			 inputRating = new Integer(ratings[1]);
            		toInclude = toInclude && inputRating > minInputRating;
            		 
            		 if (toInclude) {
	            		//itemID
	            		keyOut.set(itemID, two);
	            		
	            		//userID, rating
	               		valOut.initialize();
	            		valOut.add(ratings[0],  inputRating, two);
	       	   			context.write(keyOut, valOut);
            		 }
               	}
        	} else  if (isRatingStatFileSplit) {
        		//rating stat
				//context.getCounter("Record type count", "Rating stat").increment(1);
        		int ratingStdDev = Integer.parseInt(items[STD_DEV_ORD]);
        		keyOut.set(itemID, one);
           		valOut.initialize();
        		valOut.add(ratingStdDev,   one);
   	   			context.write(keyOut, valOut);
        	} else {
        		//if correlation is above min threshold
				//context.getCounter("Record type count", "Correlation").increment(1);
        		correlation =  Integer.parseInt( items[2]);
        		correlationLength =  Integer.parseInt(items[3]);
        		if (correlation > minCorrelation) {
	        		//correlation of 1st item
	        		keyOut.set(items[0], zero);
	        		valOut.initialize();
	   	   			if (linearCorrelation) {
	   	   				//other itemID, correlation, intersection length (weight)
	   	   				valOut.add(items[1], correlation, correlationLength, zero);
	   	   			} else {
	   	   				//other itemID, correlation, intersection length (weight)
	   	   				valOut.add(items[1], -correlation, correlationLength, zero);
	   	   			}
	   	   			context.write(keyOut, valOut);
	
	   	   			//correlation of second item
	   	   			keyOut.set(items[1], zero);
	        		valOut.initialize();
	   	   			if (linearCorrelation) {
	   	   				//other itemID, correlation, intersection length (weight)
	   	   				valOut.add(items[0], correlation, correlationLength, zero);
	   	   			} else {
	   	   				//other itemID, correlation, intersection length (weight)
	   	   				valOut.add(items[0], -correlation, correlationLength, zero);
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
    public static class PredictorReducer extends Reducer<TextInt, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valueOut = new Text();
    	private List<Tuple> ratingCorrelations = new ArrayList<Tuple>();
    	private boolean linearCorrelation;
    	private int correlationScale;
    	private int maxRating;
    	private String userID;
    	private String itemID;
    	private int rating;
    	private int ratingCorr;
    	private int weight;
    	private long logCounter = 0;
    	private double correlationModifier;
    	private Tuple ratingStat;
    	private int ratingStdDev;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	linearCorrelation = context.getConfiguration().getBoolean("correlation.linear", true);
        	correlationScale = context.getConfiguration().getInt("correlation.linear.scale", 1000);
           	maxRating = context.getConfiguration().getInt("max.rating", 100);
           	correlationModifier = context.getConfiguration().getFloat("correlation.modifier", (float)1.0);
        } 	
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(TextInt  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	ratingCorrelations.clear();
        	++logCounter;
        	ratingStat = null;
           	for(Tuple value : values) {
           		if ( ((Integer)value.get(value.getSize()-1)) == 0) {
           			//in rating correlation
           			ratingCorrelations.add(value.createClone());
					//context.getCounter("Predictor", "Rating correlation").increment(1);
           		} else if  ( ((Integer)value.get(value.getSize()-1)) == 1 )  {
           			//rating stat
           			ratingStat = value.createClone();
           		} else {
           			//in user rating
           			if (!ratingCorrelations.isEmpty()) {
	           			String userID = value.getString(0);
	           			rating = value.getInt(1);
	           			
	    				//all rating correlations
	           			for (Tuple  ratingCorrTup : ratingCorrelations) { 
        					//context.getCounter("Predictor", "User rating").increment(1);
	           				itemID = ratingCorrTup.getString(0);
	           				ratingCorr = ratingCorrTup.getInt(1);
	           				weight = ratingCorrTup.getInt(2);
	           				
	           				modifyCorrelation();
	           				int predRating = linearCorrelation? (rating * ratingCorr) / maxRating : 
	           					(rating  * correlationScale + ratingCorr) /maxRating ;
	           				if (predRating > 0) {
	           					//userID, itemID, predicted rating, correlation length, correlation coeff, input rating std dev
	           					ratingStdDev = ratingStat != null ? ratingStat.getInt(0) :  -1;
	           					valueOut.set(userID + fieldDelim + itemID + fieldDelim + predRating + fieldDelim + weight + 
	           							fieldDelim +ratingCorr  + fieldDelim + ratingStdDev);
	           					context.write(NullWritable.get(), valueOut);
	        					//context.getCounter("Predictor", "Rating correlation").increment(1);
	           				}
	           			}
           			}
           		}
           	}        	
        }
        
        
        /**
         * 
         */
        private void modifyCorrelation() {
        	double ratingCorrDb  =( (double)ratingCorr) / correlationScale;
        	ratingCorrDb = Math.pow(ratingCorrDb, correlationModifier);
        	ratingCorr = (int)(ratingCorrDb * correlationScale);
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
        int exitCode = ToolRunner.run(new UtilityPredictor(), args);
        System.exit(exitCode);
    }
   
}
