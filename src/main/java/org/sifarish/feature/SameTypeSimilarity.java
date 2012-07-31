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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.sifarish.feature.DiffTypeSimilarity.IdPairGroupComprator;
import org.sifarish.feature.DiffTypeSimilarity.IdPairPartitioner;
import org.sifarish.util.Entity;
import org.sifarish.util.Event;
import org.sifarish.util.Field;
import org.sifarish.util.Location;
import org.sifarish.util.TimeWindow;
import org.sifarish.util.Utility;



/**
 * Mapreduce for finding similarities between same type of entities with fixed set of attributes. For example, 
 * products   where the attributes are the different product features
 *  
 * @author pranab
 *
 */
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
        
        job.setMapOutputKeyClass(TextIntInt.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(IdPairGroupComprator.class);
        job.setPartitionerClass(IdPairPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SameTypeSimilarity(), args);
        System.exit(exitCode);
    }
    
    /**
     * @author pranab
     *
     */
    public static class SimilarityMapper extends Mapper<LongWritable, Text, TextIntInt, Text> {
        private TextIntInt keyHolder = new TextIntInt();
        private Text valueHolder = new Text();
        private SingleTypeSchema schema;
        private int bucketCount;
        private int hash;
        private int idOrdinal;
        private String fieldDelimRegex;
        private  int partitonOrdinal;
        private int hashPair;
 
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
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
        	System.out.println("bucketCount: " + bucketCount + "partitonOrdinal: " + partitonOrdinal  + "idOrdinal:" + idOrdinal );

       }
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            
           String partition = partitonOrdinal >= 0 ? items[partitonOrdinal] :  "none";
            		
    		hash = (items[idOrdinal].hashCode() %  bucketCount  + bucketCount) / 2 ;
    		for (int i = 0; i < bucketCount;  ++i) {
    			if (i < hash){
       				hashPair = hash * 1000 +  i;
       				keyHolder.set(partition, hashPair,0);
       				valueHolder.set("0" + value.toString());
       	   		 } else {
    				hashPair =  i * 1000  +  hash;
       				keyHolder.set(partition, hashPair,1);
       				valueHolder.set("1" + value.toString());
    			} 
   	   			context.write(keyHolder, valueHolder);
    		}
        }
    	
    }
    
    /**
     * @author pranab
     *
     */
    public static class SimilarityReducer extends Reducer<TextIntInt, Text, NullWritable, Text> {
        private Text valueHolder = new Text();
        private List<String> valueList = new ArrayList<String>();
        private Set<String> valueSet = new HashSet<String>();
        private SingleTypeSchema schema;
       private String firstId;
        private String  secondId;
        private int dist;
        private int idOrdinal;
        private String fieldDelimRegex;
        private int scale;
        private DistanceStrategy distStrategy;
        private DynamicAttrSimilarityStrategy textSimStrategy;
        private String subFieldDelim;
      
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
            String filePath = conf.get("same.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, SingleTypeSchema.class);
            schema.processStructuredFields();
        	
            idOrdinal = schema.getEntity().getIdField().getOrdinal();
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
        	scale = context.getConfiguration().getInt("distance.scale", 1000);
        	distStrategy = schema.createDistanceStrategy(scale);
        	textSimStrategy = schema.createTextSimilarityStrategy();
        	subFieldDelim = context.getConfiguration().get("sub.field.delim.regex", ":");
      }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(TextIntInt  key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	valueList.clear();
        	int secondPart = key.getSecond().get();
        	if (secondPart/1000 == secondPart%1000){
        		//same hash bucket
	        	for (Text value : values){
	        		String valSt = value.toString();
	        		valueList.add(valSt.substring(1));
	        	}
	        	
	        	for (int i = 0;  i < valueList.size();  ++i){
	        		String first = valueList.get(i);
	        		firstId =  first.split(fieldDelimRegex)[idOrdinal];
	        		for (int j = i+1;  j < valueList.size();  ++j) {
	            		String second = valueList.get(j);
	            		secondId =  second.split(fieldDelimRegex)[idOrdinal];
	            		if (!firstId.equals(secondId)){
		        			dist  = findDistance( first,  second,  context);
		   					valueHolder.set(firstId + "," + secondId + "," + dist);
		   					context.write(NullWritable.get(), valueHolder);
	            		} else {
	    					context.getCounter("Distance Data", "Same ID").increment(1);
	    					System.out.println("Repeat:" + firstId );
	            		}
	   				}
	        	}
        	} else {
        		//different hash bucket
	        	for (Text value : values){
	        		String valSt = value.toString();
	        		if (valSt.startsWith("0")) {
	        			valueList.add(valSt.substring(1));
	        		} else {
	        			String second = valSt.substring(1);
	            		secondId =  second.split(fieldDelimRegex)[idOrdinal];
	            		for (String first : valueList){
	                		firstId =  first.split(fieldDelimRegex)[idOrdinal];
		        			dist  = findDistance( first,  second,  context);
		   					valueHolder.set(firstId + "," + secondId + "," + dist);
		   					context.write(NullWritable.get(), valueHolder);
	            		}
	        		}
	        	}
        	}
        	
        }    
        
        /**
         * @param first
         * @param second
         * @param context
         * @return
         * @throws IOException 
         */
        private int findDistance(String first, String second, Context context) throws IOException {
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
	    				//categorical
	    				dist = field.findDistance(firstAttr, secondAttr);
	    			} else if (field.getDataType().equals("int")) {
	    				//int
	    				dist = numericDistance(field,  firstAttr,  secondAttr,  true, context);
	    			} else if (field.getDataType().equals("double")) {
	    				//double
	    				dist =  numericDistance( field,  firstAttr,  secondAttr, false, context);
	    			} else if (field.getDataType().equals("text")) { 
	    				//text
	    				dist = textSimStrategy.findDistance(firstAttr, secondAttr);	    				
	    			} else if (field.getDataType().equals("timeWindow")) {
	    				//time window
	    				dist = timeWindowDistance(field, firstAttr,  secondAttr, context);
	    			}  else if (field.getDataType().equals("location")) {
	    				//location
	    				dist = locationDistance(field, firstAttr,  secondAttr, context);
	    			}  else if (field.getDataType().equals("event")) {
	    				//event
	    				dist = eventDistance(field, firstAttr,  secondAttr, context);
	    			}
    			}
				distStrategy.accumulate(dist, field.getWeight());
    		}  
    		
    		netDist = distStrategy.getSimilarity();
    		return netDist;
        }
        
        /**
         * @param field
         * @param firstAttr
         * @param secondAttr
         * @param context
         * @return
         */
        private double numericDistance(Field field, String firstAttr,  String secondAttr, boolean isInt, Context context) {
        	double dist = 0;
			String[] firstValItems = firstAttr.split("\\s+");
			String[] secondValItems = secondAttr.split("\\s+");
			boolean valid = false;
    		String unit = field.getUnit();

    		if (firstValItems.length == 1 && secondValItems.length == 1){
				valid = true;
			} else if (firstValItems.length == 2 && secondValItems.length == 2 && 
					firstValItems[1].equals(unit) && secondValItems[1].equals(unit)) {
				valid = true;
			}
			
			if (valid) {
				try {
					if (isInt) {
	    				dist = field.findDistance(Integer.parseInt(firstValItems[0]), Integer.parseInt(secondValItems[0]), 
	        					schema.getNumericDiffThreshold());
					} else {
						dist = field.findDistance(Double.parseDouble(firstValItems[0]), Double.parseDouble(secondValItems[0]), 
							schema.getNumericDiffThreshold());
					}
				} catch (NumberFormatException nfEx) {
					context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
				}
			} else {
			}
        	return dist;
        }       
        
        /**
         * @param field
         * @param firstAttr
         * @param secondAttr
         * @param context
         * @return
         */
        private double timeWindowDistance(Field field, String firstAttr, String secondAttr,Context context) {
        	double dist = 0;
    		try {
    			String[] subFields = firstAttr.split(subFieldDelim);
    			TimeWindow firstTimeWindow = new TimeWindow(subFields[0], subFields[1]);
    			subFields = secondAttr.split(subFieldDelim);
    			TimeWindow secondTimeWindow = new TimeWindow(subFields[0], subFields[1]);

    			dist = field.findDistance(firstTimeWindow, secondTimeWindow);
    		} catch (ParseException e) {
    			context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
    		}
        	return dist;
        }    
        
        /**
         * @param field
         * @param firstAttr
         * @param secondAttr
         * @param context
         * @return
         */
        private double locationDistance(Field field, String firstAttr, String secondAttr,Context context) {
        	double dist = 0;
			String[] subFields = firstAttr.split(subFieldDelim);
			Location firstLocation  = new Location( subFields[0], subFields[1], subFields[2]); 
		    subFields = secondAttr.split(subFieldDelim);
			Location secondLocation  = new Location( subFields[0], subFields[1], subFields[2]); 

			dist = field.findDistance(firstLocation, secondLocation);
        	return dist;
        }    

        
        /**
         * @param field
         * @param firstAttr
         * @param secondAttr
         * @param context
         * @return
         */
        private double eventDistance(Field field, String firstAttr, String secondAttr,Context context) {
        	double dist = 0;
    		try {
    			double[] locationWeights = schema.getLocationComponentWeights();
    			String[] subFields = firstAttr.split(subFieldDelim);
    			String description = subFields[0];
    			Location location  = new Location( subFields[1], subFields[2], subFields[3]); 
    			TimeWindow timeWindow = new TimeWindow(subFields[4], subFields[5]);
    			Event firstEvent = new Event(description, location, timeWindow, locationWeights);
    			
    			subFields = secondAttr.split(subFieldDelim);
    			description = subFields[0];
    			location  = new Location( subFields[1], subFields[2], subFields[3]); 
    			timeWindow = new TimeWindow(subFields[4], subFields[5]);
    			Event secondEvent = new Event(description, location, timeWindow, locationWeights);
 
    			dist = field.findDistance(firstEvent, secondEvent);
    		} catch (ParseException e) {
    			context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
    		}
        	return dist;
        }    

    
    }    
    
    /**
     * @author pranab
     *
     */
    public static class IdPairPartitioner extends Partitioner<TextIntInt, Text> {
	     @Override
	     public int getPartition(TextIntInt key, Text value, int numPartitions) {
	    	 //consider only base part of  key
		     return key.hashCodeBase() % numPartitions;
	     }
   
   }
   
    /**
     * @author pranab
     *
     */
    public static class IdPairGroupComprator extends WritableComparator {
    	protected IdPairGroupComprator() {
    		super(TextIntInt.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		//consider only the base part of the key
    		TextIntInt t1 = (TextIntInt)w1;
    		TextIntInt t2 = (TextIntInt)w2;
    		
    		int comp = t1.compareToBase(t2);
    		return comp;
    	}
     }

}
