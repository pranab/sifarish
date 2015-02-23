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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Aggregates item predicted rating and attribute aggregator. Used by attribute bases diversifier
 * @author pranab
 *
 */
public class ItemRatingAttributeAggregator  extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Item predicted rating and attribute aggregator MR";
        job.setJobName(jobName);
        
        job.setJarByClass(ItemRatingAttributeAggregator.class);
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ItemRatingAttributeAggregator.ItemAggregatorMapper.class);
        job.setReducerClass(ItemRatingAttributeAggregator.ItemAggregatorReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("iraa.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    /**
     * @author pranab
     *
     */
    public static class ItemAggregatorMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
    	private String fieldDelimRegex;
    	private Tuple keyOut = new Tuple();
    	private Tuple valOut = new Tuple();
    	private boolean isMetaDataFileSplit;
    	private String itemID;
    	private int[] attrOrdinals;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	String metaDataFilePrefix = config.get("item.metadta.file.prefix", "meta");
        	isMetaDataFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(metaDataFilePrefix);
        	attrOrdinals = Utility.intArrayFromString(config.get("item.attr.ordinals"));
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
           	if (isMetaDataFileSplit) {
           		//items attributes
           		itemID = items[0];
           		keyOut.add(itemID, 1);
           		valOut.append(1);
           		for (int ordinal :  attrOrdinals) {
           			valOut.append(items[ordinal]);
           		}
           		
           	} else {
           		//predicted rating
           		itemID = items[1];
           		keyOut.add(itemID, 0);
           		valOut.add(0,items[0], items[2]);
           	}
           	context.write(keyOut, valOut);

        }
    }

    /**
     * @author pranab
     *
     */
    public static class ItemAggregatorReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private List<String> users = new ArrayList<String>();
    	private String itemID;
		private StringBuilder stBld =  new StringBuilder();

    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	users.clear();
        	itemID = key.getString(0);
        	for(Tuple value : values) {
        		int type = value.getInt(0);
        		if (0 == type) {
        			//users for which this item has predicted ratings
        			users.add(value.getString(1));
        		} else {
        			//item attributes
        			String attrs = value.toString(1);
        			for (String user : users) {
        				stBld.delete(0, stBld.length());
        				stBld.append(user).append(fieldDelim).append(itemID).append(fieldDelim).append(attrs);
        				valOut.set(stBld.toString());
        				
                		//userID, itemID, item attribute, ...,..
        				context.write(NullWritable.get(), valOut);
        			}
        		}
        	}
        }       
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ItemRatingAttributeAggregator(), args);
        System.exit(exitCode);
    }
    
}
