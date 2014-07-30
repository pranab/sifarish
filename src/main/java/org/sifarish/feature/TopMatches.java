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

package org.sifarish.feature;

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
import org.chombo.util.SecondarySort;
import org.chombo.util.TextInt;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Top match map reduce based on distance with neighbors
 * @author pranab
 *
 */
public class TopMatches extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Top n matches MR";
        job.setJobName(jobName);
        
        job.setJarByClass(TopMatches.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(TopMatches.TopMatchesMapper.class);
        job.setReducerClass(TopMatches.TopMatchesReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TupleTextPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());
        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class TopMatchesMapper extends Mapper<LongWritable, Text, Tuple, Text> {
		private String srcEntityId;
		private String trgEntityId;
		private int rank;
		private Tuple outKey = new Tuple();
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String fieldDelim;

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
           	fieldDelim = context.getConfiguration().get("field.delim", ",");
            fieldDelimRegex = context.getConfiguration().get("field.delim.regex", ",");
        }    

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            
            srcEntityId = items[0];
            trgEntityId = items[1];
            rank = Integer.parseInt(items[items.length - 1]);
            outKey.initialize();
            outKey.add(srcEntityId, rank);
 	        outVal.set(trgEntityId + fieldDelim + items[items.length - 1]);
 			context.write(outKey, outVal);
        }
	}
	
    /**
     * @author pranab
     *
     */
    public static class TopMatchesReducer extends Reducer<Tuple, Text, NullWritable, Text> {
    	private boolean nearestByCount;
    	private int topMatchCount;
    	private int topMatchDistance;
		private String srcEntityId;
		private int count;
		private int distance;
		private Text outVal = new Text();
        private String fieldDelim;
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
           	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	nearestByCount = context.getConfiguration().getBoolean("nearest.by.count", true);
        	if (nearestByCount) {
        		topMatchCount = context.getConfiguration().getInt("top.match.count", 10);
        	} else {
        		topMatchDistance = context.getConfiguration().getInt("top.match.distance", 200);
        	}
        }
    	
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		srcEntityId  = key.getString(0);
    		count = 0;
        	for (Text value : values){
        		//count based neighbor
				if (nearestByCount) {
					outVal.set(srcEntityId +fieldDelim + value.toString());
					context.write(NullWritable.get(), outVal);
	        		if (++count == topMatchCount){
	        			break;
	        		}
				} else {
					//distance based neighbor
					distance =Integer.parseInt( value.toString().split(",")[2]);
					if (distance  <=  topMatchDistance ) {
							outVal.set(srcEntityId + "," + value.toString());
							context.write(NullWritable.get(), outVal);
					} else {
						break;
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
        int exitCode = ToolRunner.run(new TopMatches(), args);
        System.exit(exitCode);
	}
}
