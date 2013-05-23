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
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Estimates implicit rating based on user engagement bahavior with items 
 * @author pranab
 *
 */
public class ImplicitRatingEstimator   extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Implicit rating estimator MR";
        job.setJobName(jobName);
        
        job.setJarByClass(ImplicitRatingEstimator.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ImplicitRatingEstimator.RatingEstimatorMapper.class);
        job.setReducerClass(ImplicitRatingEstimator.RatingEstimatorReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TupleIntPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }

    /**
     * @author pranab
     *
     */
    public static class RatingEstimatorMapper extends Mapper<LongWritable, Text, Tuple, IntWritable> {
    	private String fieldDelim;
    	private Tuple keyOut = new Tuple();
    	private IntWritable  valOut = new IntWritable();
    	private int  eventType = 0;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim.regex", ",");
        }    
   
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	String[] items = value.toString().split(fieldDelim);
           	eventType = Integer.parseInt(items[2]);
           	
           	keyOut.initialize();
           	keyOut.add(items[0], items[1], eventType);
           	valOut.set(eventType);
           	context.write(keyOut, valOut);
        }       
    }    
    
    /**
     * @author pranab
     *
     */
    public static class RatingEstimatorReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private int rating;
    	private EngagementToPreferenceMapper ratingMapper;
    	private int mostEngagingEventType ;
    	private int count;

    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        	String confpath = config.get("rating.mapper.config.path");
        	InputStream fs  = Utility.getFileStream( config, confpath); 
            ObjectMapper mapper = new ObjectMapper();
            ratingMapper = mapper.readValue(fs, EngagementToPreferenceMapper.class);
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        	boolean first = true;
        	count = 0;
        	for(IntWritable value : values) {
        		if (first) {
        			mostEngagingEventType = value.get();
        			++count;
        			first = false;
        		} else {
        			//all occurences of the first event type
        			if (value.get() == mostEngagingEventType) {
        				++count;
        			} else {
        				break;
        			}
        		}
        	}     
        	
        	
        	rating =ratingMapper.scoreForEvent(mostEngagingEventType, count);;
        	valOut.set(key.getString(0) + fieldDelim + key.getString(1) + fieldDelim + rating);
        }       
        
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ImplicitRatingEstimator(), args);
        System.exit(exitCode);
    }
   
}
