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
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.IntPair;
import org.chombo.util.TextPair;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class SlopeOneRating extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Slope one rating predictor MR";
        job.setJobName(jobName);
        
        job.setJarByClass(SlopeOneRating.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SlopeOneRating.SlopeOneMapper.class);
        job.setReducerClass(SlopeOneRating.SlopeOneReducer.class);
        
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntPair.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        Utility.setConfiguration(job.getConfiguration());
        int numReducer = job.getConfiguration().getInt("sor.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    /**
     * @author pranab
     *
     */
    public static class SlopeOneMapper extends Mapper<LongWritable, Text, TextPair, IntPair> {
    	private String fieldDelim;
    	private TextPair keyOut = new TextPair();
    	private IntPair valOut = new IntPair();
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
           	String[] items = value.toString().split(fieldDelim);
           	keyOut.set(items[0], items[1]);   	
           	valOut.set(new Integer(items[2]), new Integer(items[3]));
	   		context.write(keyOut, valOut);
        }   
    }
    
    /**
     * @author pranab
     *
     */
    public static class SlopeOneReducer extends Reducer<TextPair, IntPair, NullWritable, Text> {
    	private String fieldDelim;
    	private int sum ;
    	private int sumWt;
    	private int avRating;
    	private Text valueOut = new Text();
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        } 	
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(TextPair  key, Iterable<IntPair> values, Context context)
        throws IOException, InterruptedException {
        	sum = sumWt = 0;
        	for(IntPair value : values) {
        		sum += value.getFirst().get() * value.getSecond().get();
        		sumWt += value.getSecond().get();;
        	}
        	avRating = sum / sumWt;
        	valueOut.set(key.getFirst() + fieldDelim + key.getSecond() + fieldDelim +avRating);
	   		context.write(NullWritable.get(), valueOut);
        }
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SlopeOneRating(), args);
        System.exit(exitCode);
    }
    
}
