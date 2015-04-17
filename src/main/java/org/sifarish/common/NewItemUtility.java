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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

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
        	String metaDataFilePrefix = config.get("user.item.metadta.file.prefix", "meta");
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
        }
   	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	userID = key.getString(0);
        	
        	for(Tuple value : values) {
        		int type = value.getInt(0);
        		if (0 == type) {
        			//predicted ratings with item attributes
        			
        		} else {
        			//item attributes
        		}
        	}
        }
    }
    
}
