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
import java.util.HashMap;
import java.util.List;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.sifarish.feature.RecordDistanceFinder;
import org.sifarish.feature.SingleTypeSchema;

/**
 * Solves cold start problem with new items. Finds predicted rating based on items recommended
 * for an user and content based correlation of those items with the new items
 * @author pranab
 *
 */
public class NewItemUtility extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "new item utility estimator  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(NewItemUtility.class);
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NewItemUtility.ItemUtilityMapper.class);
        job.setReducerClass(NewItemUtility.ItemUtilityReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("niu.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    /**
     * @author pranab
     *
     */
    public static class ItemUtilityMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
    	private String fieldDelimRegex;
    	private Tuple keyOut = new Tuple();
    	private Tuple valOut = new Tuple();
    	private boolean isMetaDataFileSplit;
    	private String userD;
    	private String attrs;
    	private int hashBucketCount;
    	private String itemID;
    	private String userID;
    	private String[] items;
    	private int[] attrOrdinals;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	String metaDataFilePrefix = config.get("new.item.metadta.file.prefix", "new");
        	isMetaDataFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(metaDataFilePrefix);
        	hashBucketCount = config.getInt("hash.bucket.count", 16);
        	if (null != config.get("item.attr.ordinals")) {
        		attrOrdinals = Utility.intArrayFromString(config.get("item.attr.ordinals"));
        	}

        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	items = value.toString().split(fieldDelimRegex);
          	if (isMetaDataFileSplit) {
          		//item meta data
          		itemID = items[0];
          		for (int i =0; i < hashBucketCount; ++i) {
                   	keyOut.initialize();
                   	keyOut.add(i, 1);
                   	
          			valOut.initialize();
          			valOut.add(1, itemID);
          			
               		if (null != attrOrdinals) {
               			//selected attributes
               			for (int ordinal :  attrOrdinals) {
               				valOut.append(items[ordinal]);
               			}
               		} else {
               			//all attributes
               			for (int ordinal = 1; ordinal < items.length; ++ordinal) {
               				valOut.append(items[ordinal]);
               			}
               		}
               		context.write(keyOut, valOut);
          		}

          	} else {
          		//user item rating aggregated with item meta data
          		userID = items[0];
          		int hash = userID.hashCode();
          		int bucket =  (hash < 0 ?  -hash :  hash) % hashBucketCount;
               	keyOut.initialize();
               	keyOut.add(bucket, 0);
               	
               	valOut.initialize();
               	for (int i = 0; i < items.length; ++i) {
               		if (i  == items.length -1) {
               			valOut.append(Integer.parseInt(items[i]));
               		} else {
               			valOut.append(items[i]);
               		}
               	}
          		
           		context.write(keyOut, valOut);
          	}
        }    
    }
    
    /**
     * @author pranab
     *
     */
    public static class ItemUtilityReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private String userID;
    	private String itemID;
    	private int rating;
    	private Map<String, List<RatedItemWithAttributes>> itemsForUsers = new HashMap<String, List<RatedItemWithAttributes>>();
    	private Map<String, List<RatedItem>> newItemsForUsers = new HashMap<String, List<RatedItem>>();
    	private List<RatedItemWithAttributes> newItems = new ArrayList<RatedItemWithAttributes>();
    	private String[] attrs;
    	private SingleTypeSchema schema;
    	private int scale;
    	private int  distThreshold;
    	private RecordDistanceFinder distFinder;
    	private int[] newItemPredRatings;
    	private String ratingAggrStrategy;
    	private StringBuilder stBld =  new StringBuilder();
 		private static final Logger LOG = Logger.getLogger(NewItemUtility.ItemUtilityReducer.class);

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
        	try {
				schema = org.sifarish.util.Utility.getSameTypeSchema( config);
			} catch (Exception e) {
				throw new IOException("failed to process schema " + e.getMessage());
			}
        	scale = config.getInt("distance.scale", 1000);
        	distThreshold = config.getInt("dist.threshold", scale);
        	distFinder = new RecordDistanceFinder( config.get("field.delim.regex", ","),  0, scale,
        			distThreshold, schema, ":");
        	ratingAggrStrategy = config.get("new.item.rating.aggregator.strategy", "average");
        	
        }
   	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	userID = key.getString(0);
        	itemsForUsers.clear();
        	newItems.clear();
        	newItemsForUsers.clear();
        	
        	for(Tuple value : values) {
        		int type = value.getInt(0);
        		if (0 == type) {
        			//predicted ratings with item attributes
        			userID = value.getString(0);
        			int size = value.getSize();
        			rating = value.getInt(size -1);
        			attrs = value.subTupleAsArray(1, size-1);
        			List<RatedItemWithAttributes> items = itemsForUsers.get(userID);
        			if (null == items) {
        				items = new  ArrayList<RatedItemWithAttributes>();
        				itemsForUsers.put(userID, items);
        			}
        			items.add(new RatedItemWithAttributes(value.getString(1),  rating, attrs));
        		} else {
        			//new item attributes
        			attrs = value.getTupleAsArray();
        			newItems.add(new RatedItemWithAttributes(value.getString(0),  0, attrs));
        		}
        	}
        	
        	//all users
        	 for (String userID : itemsForUsers.keySet()) {
        		 //new items
        		 for (RatedItemWithAttributes newItem : newItems) {
        			 List<RatedItemWithAttributes> items = itemsForUsers.get(userID);
        			 newItemPredRatings = new int[items.size()];
        			 int i = 0;
        			 //items or an user
        			 for (RatedItemWithAttributes item : items) {
        				 newItemPredRatings[i++] = 
        						 (scale - distFinder.findDistance(item.getAttributeArray(), newItem.getAttributeArray())) * item.getRight();
        			 }
        			 int aggrRating = aggregateRating();
        			 List<RatedItem> ratedNewItems = newItemsForUsers.get(userID);
        			 if (null == userID) {
        				 ratedNewItems = new ArrayList<RatedItem>();
        				 newItemsForUsers.put(userID, ratedNewItems);
        			 }
        			 ratedNewItems.add(new RatedItem(newItem.getLeft(),  aggrRating ));
        		 }
        	 }
        	 
        	 //output ratings
        	 outputRating(context);
        }
        
       /**
     * @return
     */
        private int aggregateRating() {
        	int aggrRating = 0;
        	if (ratingAggrStrategy.equals("average")) {
        		int sum = 0;
    		    for (int rating : newItemPredRatings) {
    		    	sum += rating;
    		    }
    		    aggrRating = sum / (newItemPredRatings.length * scale);
    	   } else if  (ratingAggrStrategy.equals("average")) {
    		   int max = -1;
   		    	for (int rating : newItemPredRatings) {
   		    		if (rating > max) {
   		    			max = rating;
   		    		}
   		    	}    		   
   		    	aggrRating = max;
    	   } else {
    		   throw new IllegalArgumentException("invalid rating aggregation function");
    	   }
    	   
    	   return aggrRating;
        }
        
    
    /**
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
        private void outputRating(Context context) throws IOException, InterruptedException {
        	//all users
        	for (String userID : itemsForUsers.keySet()) {
        		List<RatedItemWithAttributes> items = itemsForUsers.get(userID);
    		
        		//existing items
        		for (RatedItemWithAttributes item : items) {
    	    		stBld.delete(0, stBld.length());
    	    		stBld.append(userID).append(fieldDelim).append(item.getLeft()).append(fieldDelim).
    	    			append(item.getRight()).append("E");
    	    		valOut.set(stBld.toString());
    	    		context.write(NullWritable.get(), valOut);
        		}
    		 
        		//new items
        		List<RatedItem> newItems = newItemsForUsers.get(userID);
        		for (RatedItem item : newItems) {
        			stBld.delete(0, stBld.length());
        			stBld.append(userID).append(fieldDelim).append(item.getLeft()).append(fieldDelim).
   	    				append(item.getRight()).append("N");
        			valOut.set(stBld.toString());
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
        int exitCode = ToolRunner.run(new NewItemUtility(), args);
        System.exit(exitCode);
    }
    
}
