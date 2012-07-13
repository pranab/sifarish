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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.TextInt;
import org.chombo.util.Utility;

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
        
        job.setMapOutputKeyClass(TextInt.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setGroupingComparatorClass(IdRankGroupComprator.class);
        job.setPartitionerClass(IdRankPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	public static class TopMatchesMapper extends Mapper<LongWritable, Text, TextInt, Text> {
		private String srcEntityId;
		private String trgEntityId;
		private int rank;
		private TextInt outKey = new TextInt();
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String fieldDelim;
        private boolean classify;
        private String firstClassAttr;
        private String secondClassAttr;

        protected void setup(Context context) throws IOException, InterruptedException {
           	fieldDelim = context.getConfiguration().get("field.delim", "\\[\\]");
            fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
            classify = context.getConfiguration().getBoolean("knn.classify", false);
        }    

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            
            srcEntityId = items[0];
            trgEntityId = items[1];
            rank = Integer.parseInt(items[items.length - 1]);
            outKey.set(srcEntityId, rank);
            if (classify) {
            	firstClassAttr = items[2];
            	secondClassAttr = items[3];
	            outVal.set(trgEntityId + fieldDelim + firstClassAttr +  fieldDelim + secondClassAttr +  fieldDelim + items[items.length - 1]);
            } else {
	            outVal.set(trgEntityId + fieldDelim + items[items.length - 1]);
            }
			context.write(outKey, outVal);
        }
	}
	
    public static class TopMatchesReducer extends Reducer<TextInt, Text, NullWritable, Text> {
    	private boolean nearestByCount;
    	private int topMatchCount;
    	private int topMatchDistance;
		private String srcEntityId;
		private int count;
		private int distance;
		private Text outVal = new Text();
        private String fieldDelim;
        private boolean classify;
        private List<String> neighbors = new ArrayList<String>();
        private NearestNeighborClassifier classifier;
        private String sourceClass;
    	
        protected void setup(Context context) throws IOException, InterruptedException {
           	fieldDelim = context.getConfiguration().get("field.delim", "\\[\\]");
        	nearestByCount = context.getConfiguration().getBoolean("nearest.by.count", true);
        	if (nearestByCount) {
        		topMatchCount = context.getConfiguration().getInt("top.match.count", 10);
        	} else {
        		topMatchDistance = context.getConfiguration().getInt("top.match.distance", 200);
        	}
            classify = context.getConfiguration().getBoolean("knn.classify", false);
        	if (classify) {
        		classifier = new VotingClassifier();
        	}
        }
    	
    	protected void reduce(TextInt key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		srcEntityId  = key.getFirst().toString();
    		count = 0;
    		neighbors.clear();
        	for (Text value : values){
        		//count based neighbor
				if (nearestByCount) {
					if (classify) {
						neighbors.add(value.toString());
						sourceClass = value.toString().split("*")[1];
					} else  {
						outVal.set(srcEntityId +fieldDelim + value.toString());
						context.write(NullWritable.get(), outVal);
					}
	        		if (++count == topMatchCount){
	        			break;
	        		}
				} else {
					//distance based neighbor
					distance =Integer.parseInt( value.toString().split(",")[2]);
					if (distance  <=  topMatchDistance ) {
						if (classify) {
							neighbors.add(value.toString());
							sourceClass = value.toString().split("*")[1];
						} else {
							outVal.set(srcEntityId + "," + value.toString());
							context.write(NullWritable.get(), outVal);
						}
					} else {
						break;
					}
				}
        	}
        	
        	if (classify) {
        		String predClass = classifier.classify(neighbors);
				outVal.set(srcEntityId + "," + sourceClass + "," + predClass);
				context.write(NullWritable.get(), outVal);
        	}
    	}
    	
    	
    }
	
    public static class IdRankPartitioner extends Partitioner<TextInt, Text> {
	     @Override
	     public int getPartition(TextInt key, Text value, int numPartitions) {
	    	 //consider only base part of  key
		     Text id = key.getFirst();
		     return id.hashCode() % numPartitions;
	     }
   }
    
    public static class IdRankGroupComprator extends WritableComparator {
    	protected IdRankGroupComprator() {
    		super(TextInt.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		//consider only the base part of the key
    		Text t1 = ((TextInt)w1).getFirst();
    		Text t2 = ((TextInt)w2).getFirst();
    		
    		int comp = t1.compareTo(t2);
    		return comp;
    	}
     }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TopMatches(), args);
        System.exit(exitCode);
	}
}
