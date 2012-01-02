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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.sifarish.feature.DiffTypeSimilarity.IdPairGroupComprator;
import org.sifarish.feature.DiffTypeSimilarity.IdPairPartitioner;
import org.sifarish.util.Entity;
import org.sifarish.util.Field;
import org.sifarish.util.Utility;



public class SameTypeSimilarity  extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Same type entity similarity MR";
        job.setJobName(jobName);
        
        job.setJarByClass(SameTypeSimilarity.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SameTypeSimilarity.SimilarityMapper.class);
        job.setReducerClass(SameTypeSimilarity.SimilarityReducer.class);
        
        job.setMapOutputKeyClass(TextIntPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        Utility.setConfiguration(job.getConfiguration());

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SameTypeSimilarity(), args);
        System.exit(exitCode);
    }
    
    public static class SimilarityMapper extends Mapper<LongWritable, Text, TextIntPair, Text> {
        private TextIntPair keyHolder = new TextIntPair();
        private SingleTypeSchema schema;
        private int bucketCount;
        private int hash;
        private int idOrdinal;
        private String fieldDelimRegex;
        private  int partitonOrdinal;
        private int hashPair;
 
        protected void setup(Context context) throws IOException, InterruptedException {
        	bucketCount = context.getConfiguration().getInt("bucket.count", 1000);
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
            
			Configuration conf = context.getConfiguration();
            String filePath = conf.get("same.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, SingleTypeSchema.class);
            partitonOrdinal = schema.getPartitioningColumn();
            idOrdinal = schema.getEntity().getIdField().getOrdinal();

       }
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            
           String partition = partitonOrdinal >= 0 ? items[partitonOrdinal] :  "";
            		
    		hash = items[idOrdinal].hashCode() %  bucketCount;
    		if (hash < bucketCount / 2) {
    			for (int i =  bucketCount / 2; i < bucketCount; ++i) {
    				hashPair =  i << 16  |  hash;
    				keyHolder.set(partition, hashPair);
        			context.write(keyHolder, value);
    			}
    		} else {
    			for (int i =  0; i < bucketCount / 2; ++i) {
    				hashPair = hash << 16 | i;
    				keyHolder.set(partition, hashPair);
        			context.write(keyHolder, value);
    			}
    		} 
        }
    	
    }
    
    public static class SimilarityReducer extends Reducer<TextIntPair, Text, NullWritable, Text> {
        private Text valueHolder = new Text();
        private List<String> valueList = new ArrayList<String>();
        private SingleTypeSchema schema;
       private String firstId;
        private String  secondId;
        private int dist;
        private int idOrdinal;
        private String fieldDelimRegex;
        private int scale;
        private DistanceStrategy distStrategy;
        private TextSimilarityStrategy textSimStrategy;
      
    	protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
            String filePath = conf.get("same.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, SingleTypeSchema.class);
        	
            idOrdinal = schema.getEntity().getIdField().getOrdinal();
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
        	scale = context.getConfiguration().getInt("distance.scale", 1000);
        	distStrategy = schema.createDistanceStrategy(scale);
        	textSimStrategy = schema.createTextSimilarityStrategy();
      }
        
        protected void reduce(TextIntPair  key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	valueList.clear();
        	for (Text value : values){
        		valueList.add(value.toString());
        	}   
        	
        	for (int i = 0;  i < valueList.size();  ++i){
        		String first = valueList.get(i);
        		firstId =  first.split(fieldDelimRegex)[idOrdinal];
        		for (int j = i+1;  j < valueList.size();  ++j) {
            		String second = valueList.get(j);
            		secondId =  second.split(fieldDelimRegex)[idOrdinal];
        			dist  = findDistance( first,  second,  context);
   					valueHolder.set(firstId + "," + secondId + "," + dist);
   					context.write(NullWritable.get(), valueHolder);
   				}
        	}
        }    
        
        private int findDistance(String first, String second, Context context) {
        	int netDist = 0;
    		String[] firstItems = first.split(fieldDelimRegex);
    		String[] secondItems = second.split(fieldDelimRegex);
    		double dist = 0;
    		boolean valid = false;
    		distStrategy.initialize();
    		
    		for (Field field :  schema.getEntity().getFields()) {
    			String firstAttr = "";
    			if (field.getOrdinal() < firstItems.length ){
    				firstAttr = firstItems[field.getOrdinal()];
    			} 
    			String secondAttr = "";
    			if (field.getOrdinal() < secondItems.length ){
    				secondAttr = secondItems[field.getOrdinal()];
    			}
    			String unit = field.getUnit();
    			
    			if (firstAttr.isEmpty() || secondAttr.isEmpty() ) {
					context.getCounter("Missing Data", "Field:" + field.getOrdinal()).increment(1);
    				if (schema.getMissingValueHandler().equals("default")) {
    					dist = 1.0;
    				} else {
    					continue;
    				}
    			} else {
	    			if (field.getDataType().equals("categorical")) {
	    				dist = field.findDistance(firstAttr, secondAttr);
	    			} else if (field.getDataType().equals("int")) {
	    				String[] firstValItems = firstAttr.split("\\s+");
	    				String[] secondValItems = secondAttr.split("\\s+");
	    				valid = false;
	    				if (firstValItems.length == 1 && secondValItems.length == 1){
	    					valid = true;
	    				} else if (firstValItems.length == 2 && secondValItems.length == 2 && 
	    						firstValItems[1].equals(unit) && secondValItems[1].equals(unit)) {
	    					valid = true;
	    				}
	    				
	    				if (valid)	{
	    					try {
	    						dist = field.findDistance(Integer.parseInt(firstValItems[0]), Integer.parseInt(secondValItems[0]), 
	    							schema.getNumericDiffThreshold());
	    					} catch (NumberFormatException nfEx) {
	    						context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
	    						continue;
	    					}
	    				} else {
	    					continue;
	    				}
	    			} else if (field.getDataType().equals("double")) {
	    				String[] firstValItems = firstAttr.split("\\s+");
	    				String[] secondValItems = secondAttr.split("\\s+");
	    				valid = false;
	    				if (firstValItems.length == 1 && secondValItems.length == 1){
	    					valid = true;
	    				} else if (firstValItems.length == 2 && secondValItems.length == 2 && 
	    						firstValItems[1].equals(unit) && secondValItems[1].equals(unit)) {
	    					valid = true;
	    				}
	    				
	    				if (valid) {
	    					try {
	    						dist = field.findDistance(Double.parseDouble(firstValItems[0]), Double.parseDouble(secondValItems[0]), 
	    								schema.getNumericDiffThreshold());
	    					} catch (NumberFormatException nfEx) {
	    						context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
	    						continue;
	    					}
	    				} else {
	    					continue;
	    				}
	    			} else if (field.getDataType().equals("text")) { 
	    				dist = textSimStrategy.findDistance(firstAttr, secondAttr);	    				
	    			}
    			}
				distStrategy.accumulate(dist, field.getWeight());
    		}  
    		
    		netDist = distStrategy.getSimilarity();
    		return netDist;
        }
        
        
        
    }    

}
