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
import org.chombo.util.TextTuple;
import org.chombo.util.Utility;

/**
 * Converts a correlation in exploded form to a sparse matrix form which is item ID followed by 
 * a list of ( itemID, correlation) tuples
 * @author pranab
 *
 */
public class CorrelationMatrixBuilder extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Correlation natrix builder  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(CorrelationMatrixBuilder.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        job.setMapperClass(CorrelationMatrixBuilder.MatrixBuilderMapper.class);
        job.setReducerClass(CorrelationMatrixBuilder.MatrixBuilderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextTuple.class);

        int numReducer = job.getConfiguration().getInt("cmb.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
		
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
		
	}

    /**
     * @author pranab
     *
     */
    public static class MatrixBuilderMapper extends Mapper<LongWritable, Text, Text, TextTuple> {
        private String fieldDelimRegex;
        private String subFieldDelim;
        private Text outKey = new Text();
        private TextTuple outVal = new TextTuple();
  
	    /* (non-Javadoc)
	     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
	    	subFieldDelim = config.get("sub.field.delim", ":");
	    	outVal.setFieldDelim(subFieldDelim);
	    }    
	    
	    /* (non-Javadoc)
	     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    protected void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
        	String[] items  =  value.toString().split(fieldDelimRegex);
        	
        	//emit for both items
        	outKey.set(items[0]);
        	outVal.add(items[1], items[2]);
	   		context.write(outKey, outVal);
       	
        	outKey.set(items[1]);
        	outVal.add(items[0], items[2]);
	   		context.write(outKey, outVal);
	    }    

    }

    /**
     * @author pranab
     *
     */
    public static class MatrixBuilderReducer extends Reducer<Text, TextTuple, NullWritable, Text> {
    	private String fieldDelimOut;
		private StringBuilder stBld =  new StringBuilder();;
		private Text outVal = new Text();
   	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimOut = config.get("field.delim.out", ",");
        }   	
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Text  key, Iterable<TextTuple> values, Context context)
        throws IOException, InterruptedException {
    		stBld.delete(0, stBld.length());
    		stBld.append(key.toString());
        	for (TextTuple value : values){
    	   		stBld.append(fieldDelimOut).append(value.toString());
        	}    		
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
        }        
    }    
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CorrelationMatrixBuilder(), args);
        System.exit(exitCode);
	}
    
}
