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
import org.chombo.util.TextPair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Injects business goal into rated items and figures out final net rating
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
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);

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
    public static class BusinessGoalMapper extends Mapper<LongWritable, Text, Text, Tuple> {
    	private String fieldDelim;
    	private Text keyOut = new Text();
    	private Tuple valOut = new Tuple();
    	private boolean isBizGoalFileSplit;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	String bizGoalFilePrefix = context.getConfiguration().get("bizGoal.file.prefix", "biz");
        	isBizGoalFileSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(bizGoalFilePrefix);
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	String[] items = value.toString().split(fieldDelim);
        }    
    }
    
    /**
     * @author pranab
     *
     */
    public static class BusinessGoalReducer extends Reducer<TextPair, Tuple, NullWritable, Text> {
    	private String fieldDelim;
        
    	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(TextPair  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {

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