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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Calculates per item rating statistics
 * @author pranab
 *
 */
public class ItemRatingStat extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Rating statistics  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(ItemRatingStat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ItemRatingStat.StatMapper.class);
        job.setReducerClass(ItemRatingStat.StatReducer.class);
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
    public static class StatMapper extends Mapper<LongWritable, Text,  Text, Tuple> {
    	private String fieldDelim;
    	private String subFieldDelim;
    	private String itemID;
    	private int rating;
    	private int ratingSum;
    	private int ratingSquareSum;
    	private int ratingMean;
    	private int ratingStdDev;
    	private Text keyOut = new Text();
    	private Tuple  valOut = new Tuple();
    	private int  count;
   	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	subFieldDelim = context.getConfiguration().get("subfield.delim", ":");
        }    
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	String[] items = value.toString().split(fieldDelim);
        	itemID = items[0];
        	
        	ratingSum = 0;
        	ratingSquareSum = 0;
        	for (int i = 1; i < items.length; ++ i) {
        		rating = ( Integer.parseInt(items[i].split(subFieldDelim)[1]));
        		ratingSum += rating;
        		ratingSquareSum += (rating * rating);
        	}
        	count = items.length - 1;
        	ratingMean = ratingSum / count;
        	int var = ratingSquareSum /  count -  ratingMean * ratingMean;
        	ratingStdDev = (int)Math.sqrt(var);
			
        	keyOut.set(itemID);
        	valOut.initialize();
        	valOut.add(ratingMean, ratingStdDev,  count);
   		   	context.write(keyOut, valOut);
        }   
        
    }
 
    /**
     * @author pranab
     *
     */
    public static class StatReducer extends Reducer<Text, Tuple, NullWritable, Text> {
    	private String fieldDelim;
    	private Text valOut = new Text();
    	private int maxCount;
    	private int maxRatingMean;
    	private int minRatingStdDev;
    	private int thisCount;
    	private int thisRatingMean;
    	private int thisRatingStdDev;
    	private String statSummaryFile;
    	private int ratingScale;
   	
       	/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", ",");
        	ratingScale = context.getConfiguration().getInt("rating.scale", 100);
        	statSummaryFile = config.get("stat.summary.file");
        }

	   	protected void cleanup(Context context)  
	   			throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
     	   	FileSystem dfs = FileSystem.get(config);
     	   	Path dstPath = new Path(statSummaryFile);
     	   	if (dfs.exists(dstPath)) {
        	   dfs.delete(dstPath, false);
     	   	}
           FSDataOutputStream out = dfs.create(dstPath);
           out.writeUTF("maxCount:" + maxCount + "\n");
           out.writeUTF("maxRatingMean:" + maxRatingMean + "\n");
           out.writeUTF("minRatingStdDev:" + minRatingStdDev + "\n");
           out.close();
           dfs.close();
	   	}
	   	
	   	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Text  key, Iterable<Tuple> values, Context context)
        throws IOException, InterruptedException {
        	maxCount = 0;
        	maxRatingMean = 0;
        	minRatingStdDev = ratingScale;
        	for(Tuple value : values) {
        		thisCount = value.getInt(2);
        		thisRatingMean = value.getInt(0);
        		thisRatingStdDev =  value.getInt(1);
        		if (thisCount > maxCount) {
        			maxCount = thisCount;
        		}
        		if (thisRatingMean > maxRatingMean){
        			maxRatingMean = thisRatingMean;
        		}
        		if (thisRatingStdDev < minRatingStdDev){
        			minRatingStdDev = thisRatingStdDev; 
        		}
        		
        		valOut.set(key.toString() + fieldDelim + value.toString());
    			context.write(NullWritable.get(), valOut);
        	}
        }        
    }      
  
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ItemRatingStat(), args);
        System.exit(exitCode);
    }
    
}
