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
import java.util.Set;

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
import org.chombo.util.BasicUtils;
import org.chombo.util.Pair;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.sifarish.util.Field;
import org.sifarish.util.MatchingProfile;
import org.sifarish.util.Utility;

/**
 * @author pranab
 *
 */
public class ProfileItemSimilarity extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "profile and entity similarity MR";
        job.setJobName(jobName);
        
        job.setJarByClass(ProfileItemSimilarity.class);
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ProfileItemSimilarity.SimilarityMapper.class);
        job.setReducerClass(ProfileItemSimilarity.SimilarityReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);

        Utility.setConfiguration(job.getConfiguration());

        int numReducer = job.getConfiguration().getInt("pes.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }

	/**
	 * @author pranab
	 *
	 */
	public static class SimilarityMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String subFieldDelimRegex;
        private SingleTypeSchema schema;
        private List<MatchingProfile> profiles = new ArrayList<MatchingProfile>();
        private String[] items;
        private double dist;
        private int idOrdinal;
        private String fieldVal;
        private String dataType;
        private Pair<Integer, Integer> intRange;
        private Pair<Double, Double> dblRange;
        private Set<String> valueSet;
        private int scale;
        private DistanceStrategy distStrategy;
        private DynamicAttrSimilarityStrategy textSimStrategy;
        private String itemId;
        private String profileId;
        private int fieldOrd;
        

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", ",");
        	subFieldDelimRegex = config.get("sub.field.delim.regex", ":");
        	schema = Utility.getSingleTypeSchema(config, "pes.schema.file.path");
        	
        	//load profiles 
        	List<String> lines = org.chombo.util.Utility.getFileLines(config, "pes.profile.file.path");
        	for (String line : lines) {
        		profiles.add(new MatchingProfile(line, fieldDelimRegex, subFieldDelimRegex, schema));
        	}
        	
            //ordinal of Id field
            idOrdinal = schema.getEntity().getIdField().getOrdinal();
            
        	//distance calculation strategy
        	scale = config.getInt("pes.distance.scale", 1000);
        	distStrategy = schema.createDistanceStrategy(scale);
            
        	//text field similarity calculation strategy
        	textSimStrategy = schema.createTextSimilarityStrategy();
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
	    protected void map(LongWritable key, Text value, Context context)
	    		throws IOException, InterruptedException {
        	items  = value.toString().split(fieldDelimRegex);
        	itemId = items[idOrdinal];
        	
        	//all profiles
        	for (MatchingProfile profile : profiles) {
        		profileId = profile.getId(idOrdinal);
        		
        		//all fields
        		distStrategy.initialize();
        		for (Field field :  schema.getEntity().getFields()) {
        			//if ID or class attribute field, skip it
        			if (field.isId() ||  field.isClassAttribute()) {
        				continue;
        			}
        			
        			fieldOrd = field.getOrdinal();
        			fieldVal = items[fieldOrd];
        			dataType =  field.getDataType();
        			if (dataType.equals(Field.DATA_TYPE_INT)) {
        				intRange = profile.getIntRange(fieldOrd);
        				int intVal = Integer.parseInt(fieldVal);
        				if (intVal >= intRange.getLeft() && intVal <= intRange.getRight()) {
        					dist = 0;
        				} else if (intVal < intRange.getLeft()){
    	    				dist = field.findDistance(intVal, intRange.getLeft(), schema.getNumericDiffThreshold());
        				} else {
    	    				dist = field.findDistance(intVal, intRange.getRight(), schema.getNumericDiffThreshold());
        				}
        			} else if (dataType.equals(Field.DATA_TYPE_DOUBLE)) {
        				dblRange = profile.getDoubleRange(fieldOrd);
        				double dblVal = Double.parseDouble(fieldVal);
        				if (dblVal >= dblRange.getLeft() && dblVal <= dblRange.getRight()) {
        					dist = 0;
        				} else if (dblVal < dblRange.getLeft()){
    	    				dist = field.findDistance(dblVal, dblRange.getLeft(), schema.getNumericDiffThreshold());
        				} else {
    	    				dist = field.findDistance(dblVal, dblRange.getRight(), schema.getNumericDiffThreshold());
        				}
        			} else if (dataType.equals(Field.DATA_TYPE_CATEGORICAL)) {
        				valueSet = profile.getCategoricalSet(fieldOrd);
        				if (valueSet.contains(fieldVal)) {
        					dist = 0;
        				} else {
        					//minimum distance to any in the set
        					double minDist = 2;
        					double thisDist = 0;
        					for (String thisValue : valueSet) {
        						thisDist = field.findDistance(thisValue, fieldVal);
        						if (thisDist < minDist) {
        							minDist = thisDist;
        						}
        					}
        					dist = minDist;
        				}
        			} else if (dataType.equals(Field.DATA_TYPE_TEXT)) {
	    				dist = textSimStrategy.findDistance(profile.getText(fieldOrd), fieldVal);	    				
        			} else {
        				throw new IllegalStateException("unsupported data type");
        			} 
        			
        			//aggregate attribute  distance for all entity attributes
    				distStrategy.accumulate(dist, field);
        		}
        		
        		int netDist = distStrategy.getSimilarity();
        		outKey.initialize();
        		outKey.add(profileId, netDist);
        		outVal.initialize();
        		outVal.add(itemId, netDist);
        		context.write(outKey, outVal);
        	}
        }        
	}

    /**
     * @author pranab
     *
     */
    public static class SimilarityReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelimOut;
		private StringBuilder stBld = new  StringBuilder();
		private boolean outputFormatCompact;
		private List<String> items = new ArrayList<String>();
		private boolean outputIncludesDist;
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimOut = config.get("field.delim", ",");
        	outputFormatCompact = config.getBoolean("pes.output.format.compact", true);
        	outputIncludesDist = config.getBoolean("pes.output.includes.dist", false);
        }
        
		/* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
        	items.clear();
        	for (Tuple value : values){
        		if (outputFormatCompact) {
        			items.add(value.getString(0));
        		} else {
    				stBld.delete(0, stBld.length());
    				stBld.append(key.getString(0)).append(fieldDelimOut).append(value.getString(0));
        			if (outputIncludesDist) {
        				stBld.append(fieldDelimOut).append(value.getInt(1));
        			} 
        			outVal.set(stBld.toString());
    				context.write(NullWritable.get(), outVal);
        		}
        	}
        	if (outputFormatCompact) {
        		outVal.set(key.getString(0) + BasicUtils.join(items));
   				context.write(NullWritable.get(), outVal);
        	}
        }        
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ProfileItemSimilarity(), args);
        System.exit(exitCode);
    }
    

}
