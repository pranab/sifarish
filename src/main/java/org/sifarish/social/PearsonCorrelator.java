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

import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.sifarish.common.ItemDynamicAttributeSimilarity;

public class PearsonCorrelator extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "PearsonCorrelator  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(PearsonCorrelator.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PearsonCorrelator.PearsonMapper.class);
        job.setReducerClass(PearsonCorrelator.PrearsonReducer.class);
        
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
   
    public static class PearsonMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
        private int bucketCount;
        private int hash;
        private String fieldDelimRegex;
        private Integer hashPair;
        private String itemID;
        private Tuple keyHolder = new Tuple();
        private Tuple valueHolder = new Tuple();
        private int hashPairMult;
        private int hashCode;
        private int ratingScale;
    	private String subFieldDelim;
        private static final Logger LOG = Logger.getLogger(ItemDynamicAttributeSimilarity.SimilarityMapper.class);
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
            if (conf.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             	System.out.println("in debug mode");
            }
        	bucketCount = conf.getInt("bucket.count", 10);
        	fieldDelimRegex = conf.get("field.delim.regex", "\\[\\]");
        	hashPairMult = conf.getInt("hash.pair.multiplier", 1000);
        	subFieldDelim = context.getConfiguration().get("subfield.delim", ":");
        	ratingScale = context.getConfiguration().getInt("rating.scale", 100);
      }    
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	String[] items  =  value.toString().split(fieldDelimRegex);
        	itemID = items[0];
        	hashCode = itemID.hashCode();
        	if (hashCode < 0) {
        		hashCode = - hashCode;
        	}
    		hash = (hashCode %  bucketCount) / 2 ;

    		boolean valueInitialized = false;
    		for (int i = 0; i < bucketCount;  ++i) {
    			keyHolder.initialize();
    			if (i < hash){
       				hashPair = hash * hashPairMult +  i;
       				keyHolder.add(hashPair, Utility.ZERO);
       				if (!valueInitialized) {
       					createValueTuple(Utility.ZERO,  items);
       					valueInitialized = true;
       				}
       	   		 } else {
       	   			 if (i == hash) {
       	   				 valueInitialized = false;
       	   			 }
    				hashPair =  i * hashPairMult  +  hash;
       				keyHolder.add(hashPair, Utility.ONE);
      				if (!valueInitialized) {
      					createValueTuple(Utility.ONE,  items);
       					valueInitialized = true;
      				}
    			} 
   	   			context.write(keyHolder, valueHolder);
    		}
       	
        }
        
        /**
         * @param secKey
         * @param items
         */
        private void createValueTuple(Integer secKey, String[]  items) {
        	valueHolder.initialize();
        	valueHolder.add(secKey, items[0]);
        	
        	//all userID and rating pair
        	String[] subItems = null;
        	String userID = null;
        	Integer rating = 0;
        	for (int i = 1; i < items.length; ++ i) {
        		subItems = items[i].split(subFieldDelim);
        		userID = subItems[0];
        		rating = ( Integer.parseInt(subItems[1])) *  ratingScale;
            	valueHolder.add(userID, rating);
        	}
       	
        }
    }
    
    public static class PrearsonReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
        private Text valueHolder = new Text();
        private String fieldDelim;
        private int hashPairMult;
        private int corrScale;
        private List<UserRating> userRatings = new ArrayList<UserRating>();
        
        private static final Logger LOG = Logger.getLogger(PearsonCorrelator.PrearsonReducer.class);
       
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
            if (conf.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             	System.out.println("in debug mode");
            }
        	
        	fieldDelim = conf.get("field.delim", ",");
        	hashPairMult = conf.getInt("hash.pair.multiplier", 1000);
           	corrScale = conf.getInt("correlation.scale", 1000);
        }       
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	
        	int hashPair = key.getInt(0);
    		UserRating userRating = null;
    		UserRating userRatingSecond = null;
    		int corr = 0;
        	if (hashPair / hashPairMult == hashPair % hashPairMult){
        		//same bucket
        		userRatings.clear();
        		for (Tuple tuple : values) {
        			userRating = new UserRating(tuple);
        			userRatings.add(userRating);
        		}
        		
        		//pair them
        		for (int i = 0; i < userRatings.size(); ++i) {
        			for (int j = i+1; j <  userRatings.size(); ++ j ) {
        				corr = findCorrelation(userRatings.get(i), userRatings.get(j)); 
        				if (corr > 0) {
        					valueHolder.set(userRatings.get(i).getItemID() + fieldDelim + userRatings.get(j).getItemID() + fieldDelim + corr);
		   					context.write(NullWritable.get(), valueHolder);
        				}
        			}
        		}
        	
        	} else {
        		//different bucket
        		userRatings.clear();
        		for (Tuple tuple : values) {
        			if (tuple.getInt(0) == Utility.ZERO) {
        				userRating = new UserRating(tuple);
        				userRatings.add(userRating);
        			} else {
        				userRatingSecond = new UserRating(tuple);
        				
        				//pair with each in the first set
        				for (UserRating userRatingFirst : userRatings) {
            				corr = findCorrelation(userRatingFirst,userRatingSecond); 
            				if (corr > 0) {
            					valueHolder.set(userRatingFirst.getItemID() + fieldDelim + userRatingSecond.getItemID() + fieldDelim + corr);
    		   					context.write(NullWritable.get(), valueHolder);
            				}
        					
        				}
        				
        			}
        		}
        		
        	}

        }
        
    
        /**
         * @param ratingOne
         * @param ratingTwo
         * @return
         */
        private int findCorrelation(UserRating ratingOne, UserRating ratingTwo) {
        	int corr = 0;
        	
        	ratingOne.initializeMatch();
        	ratingTwo.initializeMatch();
        	
        	//finf matching user rating
        	for (int i = 0; i < ratingOne.getRatings().size(); ++i) {
        		Pair<String, Integer> userRatingOne = ratingOne.getRatings().get(i);
        		String userIDOne = userRatingOne.getLeft();
        		
        		for(int j = 0; j <  ratingTwo.getRatings().size(); ++j) {
            		Pair<String, Integer> userRatingTwo = ratingTwo.getRatings().get(j);
            		String userIDTwo = userRatingTwo.getLeft();
        			if (userIDOne.equals(userIDTwo))  {
        				ratingOne.markMatched(i);
        				ratingTwo.markMatched(j);
        				break;
        			}
        		}
        	}
        	
        	if (ratingOne.getMatchCount() > 2) {
	        	//mean and stad dev
	        	ratingOne.calculateStat();
	        	ratingTwo.calculateStat();
	        	
	        	//co variance 
	        	int[] coVarItems = ratingOne.findCoVarianceItems(null);
	        	coVarItems = ratingOne.findCoVarianceItems(coVarItems);
	        	int coVar = 0;
	        	for (int item : coVarItems) {
	        		coVar += item;
	        	}
	        	coVar /= coVarItems.length;
	        	
	        	//pearson correlation
	        	corr = (coVar * corrScale) / (ratingOne.getRatingStdDev() * ratingTwo.getRatingStdDev());
	        	corr += corrScale;
	        	corr /= 2;
        	}
        	
        	return corr;
        }
        
        
    }    
    
    /**
     * @author pranab
     *
     */
    public static class UserRating {
    	private String itemID;
    	private List<Pair<String, Integer>> ratings = new ArrayList<Pair<String, Integer>>();
    	private List<Integer> matchedRatings = new ArrayList<Integer>();
    	private int ratingMean;
    	private int ratingStdDev;
   	
    	
		public UserRating(Tuple tuple) {
			super();
			itemID = tuple.getString(1);
			
        	for (int i = 2; i < tuple.getSize(); ) {
        		String userID = tuple.getString(i++) ;
        		Integer rating = tuple.getInt(i++);
        		ratings.add(Pair.of(userID, rating));
        	}
		}

		public String getItemID() {
			return itemID;
		}

		public List<Pair<String, Integer>> getRatings() {
			return ratings;
		}

		public void initializeMatch() {
			matchedRatings.clear();
		}
		
		public void markMatched(Integer index) {
			matchedRatings.add(index);
		}
		
		public int getMatchCount() {
			return matchedRatings.size();
		}
		
		public int getMatchedRating(int index) {
			return ratings.get(matchedRatings.get(index)).getRight();
		}
		
		public void calculateStat() {
	    	int ratingSum = 0;
	    	int ratingSquareSum = 0;
			int rating = 0;
			
			for (int index : matchedRatings) {
				rating = ratings.get(index).getRight();
				ratingSum += rating;
				ratingSquareSum += rating * rating;
			}
			ratingMean = ratingSum / matchedRatings.size();
        	int var = ratingSquareSum /  matchedRatings.size() -  ratingMean * ratingMean;
        	ratingStdDev = (int)Math.sqrt(var);
		}

		public int getRatingMean() {
			return ratingMean;
		}

		public int getRatingStdDev() {
			return ratingStdDev;
		}
		
		public int[] findCoVarianceItems(int[] coVarItems) {
			if (null == coVarItems) {
				coVarItems = new int[matchedRatings.size()];
				for (int i =0; i < matchedRatings.size(); ++i) {
					coVarItems[i] = getMatchedRating(i) - ratingMean;
				}
			} else {
				for (int i =0; i < matchedRatings.size(); ++i) {
					coVarItems[i] *= (getMatchedRating(i) - ratingMean);
				}
				
			}
			
			return coVarItems;
		}
    	
    }
}
