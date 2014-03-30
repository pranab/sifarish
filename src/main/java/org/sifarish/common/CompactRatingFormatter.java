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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Utility;

/**
 * Converts rating data from an exploded format to compact format. 
 * Compact format : item1,user1:rating1, user2:rating2
 * exploded format : user1, itemm1, rating1
 * 
 * @author pranab
 *
 */
public class CompactRatingFormatter  extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Rating compact format converter  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(CompactRatingFormatter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(CompactRatingFormatter.FormatterMapper.class);
        job.setReducerClass(CompactRatingFormatter.FormatterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
		
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

    /**
     * @author pranab
     *
     */
    public static class FormatterMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String fieldDelimRegex;
        private String subFieldDelim;
        private Text outKey = new Text();
        private Text outVal = new Text();
  
	    /* (non-Javadoc)
	     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
	    	subFieldDelim = config.get("sub.field.delim", ":");
	    }
	    /* (non-Javadoc)
	     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    protected void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
        	String[] items  =  value.toString().split(fieldDelimRegex);
        	
        	//emit itemsID as key and user: rating as value
        	outKey.set(items[1]);
        	outVal.set(items[0] + subFieldDelim + items[2]);
	   		context.write(outKey, outVal);
	    }    
    }
    
    /**
     * @author pranab
     *
     */
    public static class FormatterReducer extends Reducer<Text, Text, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private StringBuilder stBld = new StringBuilder();
    	
    	
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
        protected void reduce(Text  key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	if (stBld.length() > 0) {
        		stBld.delete(0,  stBld.length());
        	}
        	
       		stBld.append(key.toString());
           	for(Text value : values) {
           		stBld.append(fieldDelim).append(value);
           	}
           	valOut.set(stBld.toString());
    		context.write(NullWritable.get(), valOut);
        }
        
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CompactRatingFormatter(), args);
        System.exit(exitCode);
    }

}
