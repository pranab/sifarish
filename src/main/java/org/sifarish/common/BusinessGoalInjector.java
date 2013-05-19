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
import org.chombo.util.SecondarySort;
import org.chombo.util.TextPair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.omg.CORBA.portable.ValueOutputStream;
import org.sifarish.common.UtilityPredictor.ItemIdGroupComprator;
import org.sifarish.common.UtilityPredictor.ItemIdPartitioner;

/**
 * Injects business goal into rated items and figures out final net rating. The basic idea is to 
 * find a middle ground between consumer interest and business interest
 * @author pranab
 *
 */
public class BusinessGoalInjector extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Business goal injector MR";
        job.setJobName(jobName);
        
        job.setJarByClass(BusinessGoalInjector.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BusinessGoalInjector.BusinessGoalMapper.class);
        job.setReducerClass(BusinessGoalInjector.BusinessGoalReducer.class);
        
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

    /**
     * @author pranab
     *
     */
    public static class BusinessGoalMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
    	private String fieldDelim;
    	private Tuple keyOut = new Tuple();
    	private Tuple valOut = new Tuple();
    	private boolean isBizGoalFileSplit;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	String bizGoalFilePrefix = context.getConfiguration().get("biz.goal.file.prefix", "biz");
        	isBizGoalFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(bizGoalFilePrefix);
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	String[] items = value.toString().split(fieldDelim);
           	keyOut.initialize();
           	valOut.initialize();
           	if (isBizGoalFileSplit) {
           		//item ID
           		keyOut.add(items[0], 0);
           		
           		//business goal scores
           		for (int i = 1; i < items.length; ++i) {
           			valOut.add(Integer.parseInt(items[i]));
           		}
           	} else {
           		//item ID
           		keyOut.add(items[1], 1);
           		
           		//userID, score
           		valOut.add(items[0], Integer.parseInt(items[2]));
           	}
           	context.write(keyOut, valOut);
        }    
    }
    
    /**
     * @author pranab
     *
     */
    public static class BusinessGoalReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private int[] bizGoalWeights;
    	private int[] bizGoalThreshold;
        private int recWt;
        private int maxBizGoalWeight;
        private  final int  MAX_WEIGHT = 100;
        
    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        	bizGoalWeights = Utility.intArrayFromString(config.get("biz.goal.weights"),fieldDelim );
        	maxBizGoalWeight = config.getInt("max.biz.goal.weight",  70);
        	int sumWt = 0;
        	for (int wt : bizGoalWeights) {
        		sumWt += wt;
        	}
        	if (sumWt > maxBizGoalWeight) {
        		throw new IllegalArgumentException("Sum of business score weights exceed limit");
        	}
        	recWt = MAX_WEIGHT - sumWt;
        	
        	bizGoalThreshold = Utility.intArrayFromString(config.get("biz.goal.threshold"),fieldDelim );

        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	boolean first = true;
        	Tuple bizScore = null;
        	boolean toSkip = false;
        	for(Tuple value : values) {
        		toSkip = false;
        		if (first) {
        			if (value.isInt(0)) {
        				//business score available for this item
        				bizScore = value.createClone();
        			} else {
        				//just emit rating
        				valOut.set(value.getString(0) + fieldDelim + key.getString(0) + fieldDelim + value.getInt(1));
            	   		context.write(NullWritable.get(), valOut);
        			}
        			first = false;
        		} else {
        			int weightedScore = 0;
        			if (null != bizScore) {
        				//weighted average score
        				int sumWeightedScore = recWt * value.getInt(1);
        				int numBizGoal = bizScore.getSize();
        				for (int i = 0; i < numBizGoal; ++i) {
        					int score = bizScore.getInt(i);
        					if (bizGoalThreshold[i] >= 0 && score  <= bizGoalThreshold[i] ) {
        						toSkip = true;
        						break;
        					}
        					sumWeightedScore += bizGoalWeights[i] * score;
        				}
        				weightedScore = sumWeightedScore / MAX_WEIGHT;
        			} else {
        				//just  score
        				weightedScore = value.getInt(1);
        			}
        			if (!toSkip) {
        				valOut.set(value.getString(0) + fieldDelim + key.getString(0) + fieldDelim + weightedScore);
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
        int exitCode = ToolRunner.run(new BusinessGoalInjector(), args);
        System.exit(exitCode);
    }
    
}