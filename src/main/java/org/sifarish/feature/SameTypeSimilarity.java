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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.sifarish.util.Event;
import org.sifarish.util.Field;
import org.sifarish.util.HourWindow;
import org.sifarish.util.Location;
import org.sifarish.util.TimeWindow;
import org.sifarish.util.Utility;



/**
 * Mapreduce for finding similarities between same type of entities with fixed set of attributes. For example, 
 * products   where the attributes are the different product features. If single set of size n, matches are found 
 * for n x n pairs. For 2 sets of size n1 and n2, matches are for n1 x n2 pairs 
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

        int numReducer = job.getConfiguration().getInt("sts.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        
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
        private int hashCode;
   	 	private boolean interSetMatching;
   	 	private  boolean  isBaseSetSplit;
   	 	private static final int hashMultiplier = 1000;
   	 	private boolean autoGenerateId;
   	 	private String record;
        private static final Logger LOG = Logger.getLogger(SimilarityMapper.class);
 
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	bucketCount = context.getConfiguration().getInt("bucket.count", 1000);
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
            
        	//data schema
			Configuration conf = context.getConfiguration();
            String filePath = conf.get("same.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, SingleTypeSchema.class);
            
            //ordinal for partition field which partitions data
            partitonOrdinal = schema.getPartitioningColumn();
            
            //ordinal of Id field
            idOrdinal = schema.getEntity().getIdField().getOrdinal();
            
            if (conf.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }
        	LOG.debug("bucketCount: " + bucketCount + "partitonOrdinal: " + partitonOrdinal  + "idOrdinal:" + idOrdinal );
        	
        	//inter set matching
       	 	interSetMatching = conf.getBoolean("inter.set.matching",  false);
       	 	String baseSetSplitPrefix = conf.get("base.set.split.prefix", "base");
       	 	isBaseSetSplit = ((FileSplit)context.getInputSplit()).getPath().getName().startsWith(baseSetSplitPrefix);
       	 	
       	 	//generate record Id as the first field
       	 	autoGenerateId = conf.getBoolean("auto.generate.id", false);
       	 	if (autoGenerateId && idOrdinal != 0) {
       	 		throw new IllegalArgumentException("when record  Id is auto generated, it should be the first field");
       	 	}
       }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	record = value.toString();
        	if (autoGenerateId) {
        		record = org.chombo.util.Utility.generateId() + fieldDelimRegex + record;
        	} 
            String[] items  =  record.split(fieldDelimRegex);
            String partition = partitonOrdinal >= 0 ? items[partitonOrdinal] :  "N";
       		hashCode = items[idOrdinal].hashCode();
       		if (hashCode < 0) {
       			hashCode = - hashCode;
       		}
            	
            if (interSetMatching) {
            	// 2 sets
	    		hash = hashCode %  bucketCount ;
            	if (isBaseSetSplit) {
    	    		for (int i = 0; i < bucketCount;  ++i) {
	       				hashPair = hash * hashMultiplier +  i;
	       				keyHolder.set(partition, hashPair,0);
	       				valueHolder.set("0" + value.toString());
		    			LOG.debug("hashPair:" + hashPair);
		   	   			context.write(keyHolder, valueHolder);
    	    		}
            	} else {
    	    		for (int i = 0; i < bucketCount;  ++i) {
	    				hashPair =  i * hashMultiplier  +  hash;
	       				keyHolder.set(partition, hashPair,1);
	       				valueHolder.set("1" + value.toString());
		    			LOG.debug("hashPair:" + hashPair);
		   	   			context.write(keyHolder, valueHolder);
    	    		}            		
            	}
            } else {
            	// 1 set
	    		hash = (hashCode %  bucketCount) / 2 ;
	    		for (int i = 0; i < bucketCount;  ++i) {
	    			if (i < hash){
	       				hashPair = hash * hashMultiplier +  i;
	       				keyHolder.set(partition, hashPair,0);
	       				valueHolder.set("0" + value.toString());
	       	   		 } else {
	    				hashPair =  i * hashMultiplier  +  hash;
	       				keyHolder.set(partition, hashPair,1);
	       				valueHolder.set("1" + value.toString());
	    			} 
	    			LOG.debug("hashPair:" + hashPair);
	   	   			context.write(keyHolder, valueHolder);
	    		}
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
        private String fieldDelim;
        private int scale;
        private DistanceStrategy distStrategy;
        private DynamicAttrSimilarityStrategy textSimStrategy;
        private String subFieldDelim;
        private int[] facetedFields;
        private int[] passiveFields ;
        private boolean includePassiveFields;
        private String[] firstItems;
        private String[] secondItems;
        private int  distThreshold;
        private boolean  outputIdFirst ;
        private int setIdSize;
        private boolean inFirstBucket;
        private int firstBucketSize;
        private int secondBucketSize;
        private boolean mixedInSets;
        private int[]  extraOutputFields;
        private boolean outputRecord;
        private static final Logger LOG = Logger.getLogger(SimilarityReducer.class);
        
        
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
        	fieldDelimRegex = conf.get("field.delim.regex", ",");
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
			
			//schema
            String filePath = conf.get("same.schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, SingleTypeSchema.class);
            schema.processStructuredFields();
            schema.setConf(conf);
        	
            idOrdinal = schema.getEntity().getIdField().getOrdinal();
        	scale = conf.getInt("distance.scale", 1000);
        	subFieldDelim = conf.get("sub.field.delim.regex", "::");
        	
        	//distance calculation strategy
        	distStrategy = schema.createDistanceStrategy(scale);

        	//text field similarity calculation strategy
        	textSimStrategy = schema.createTextSimilarityStrategy();
        	
        	//faceted fields
        	String facetedFieldValues =  conf.get("faceted.field.ordinal");
        	if (!StringUtils.isBlank(facetedFieldValues)) {
        		facetedFields = org.chombo.util.Utility.intArrayFromString(facetedFieldValues);
        	}
        	
        	//carry along all passive fields in output
        	includePassiveFields = conf.getBoolean("include.passive.fields", false);
        	
        	//distance threshold for output
        	distThreshold = conf.getInt("dist.threshold", scale);
        	
        	//output ID first
        	outputIdFirst =   conf.getBoolean("output.id.first", true);      	

        	//inter set matching
        	mixedInSets = conf.getBoolean("mixed.in.sets",  false);
        	setIdSize = conf.getInt("set.ID.size",  0);
        	
        	//extra selected passive fields to be output
        	String extraOutputFieldList = conf.get("extra.output.field");
        	if (!StringUtils.isBlank(extraOutputFieldList)) {
        		extraOutputFields = org.chombo.util.Utility.intArrayFromString(extraOutputFieldList);
        		
        	}        	
        	
        	//output whole record
        	outputRecord =  conf.getBoolean("output.record", false);     
        	
            if (conf.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
            }
      }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(TextIntInt  key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	valueList.clear();
        	inFirstBucket = true;
        	firstBucketSize = secondBucketSize = 0;
        	int secondPart = key.getSecond().get();
        	LOG.debug("reducer key hash pair:" + secondPart);
        	if (secondPart/1000 == secondPart%1000){
        		//same hash bucket
	        	for (Text value : values){
	        		String valSt = value.toString();
	        		valueList.add(valSt.substring(1));
	        	}
	        	firstBucketSize = secondBucketSize = valueList.size();
	        	for (int i = 0;  i < valueList.size();  ++i){
	        		String first = valueList.get(i);
	        		firstId =  first.split(fieldDelimRegex)[idOrdinal];
	        		for (int j = i+1;  j < valueList.size();  ++j) {
	            		String second = valueList.get(j);
	            		secondId =  second.split(fieldDelimRegex)[idOrdinal];
	            		if (!firstId.equals(secondId)){
		        			dist  = findDistance( first,  second,  context);
		        			if (dist <= distThreshold) {
		        				valueHolder.set(createValueField(first, first));
		        				context.write(NullWritable.get(), valueHolder);
		        			}
	            		} else {
	    					//context.getCounter("Distance Data", "Same ID").increment(1);
	    					LOG.debug("Repeat:" + firstId );
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
	        			if (inFirstBucket) {
	        				firstBucketSize = valueList.size();
	        				inFirstBucket = false;
	        			}
	        			++secondBucketSize;
	        			String second = valSt.substring(1);
	            		secondId =  second.split(fieldDelimRegex)[idOrdinal];
	            		for (String first : valueList){
	                		firstId =  first.split(fieldDelimRegex)[idOrdinal];
	                		LOG.debug("ID pair:" + firstId + "  " +  secondId);
		        			dist  = findDistance( first,  second,  context);
		        			if (dist <= distThreshold) {
		        				valueHolder.set(createValueField(first, second));
		        				context.write(NullWritable.get(), valueHolder);
		        			}
	            		}
	        		}
	        	}
        	}
        	
        	LOG.debug("bucket pair:" + key.getSecond() + " firstBucketSize:" + firstBucketSize +  
        			" secondBucketSize:" + secondBucketSize);
        }    
        
        /**
         * @param first
         * @param second
         * @param context
         * @return
         * @throws IOException 
         */
        private int findDistance(String first, String second, Context context) throws IOException {
        	//LOG.debug("findDistance:" + first + "  " + second);
        	int netDist = 0;

       		//if inter set matching with mixed in sets, match only same ID from different sets
        	if (mixedInSets) {
        		//entityID is concatenation of setID and real entityID
        		String firstEntityId = firstId.substring(setIdSize);
        		String secondEntityId = secondId.substring(setIdSize);
        		if (!firstEntityId.equals(secondEntityId)) {
        			netDist =  distThreshold + 1;
					//context.getCounter("Distance Data", "Diff ID from separate sets").increment(1);
        			return netDist;
        		}
        	}
        	
    		firstItems = first.split(fieldDelimRegex);
    		secondItems = second.split(fieldDelimRegex);
    		double dist = 0;
    		boolean valid = false;
    		distStrategy.initialize();
    		List<Integer> activeFields = null;
    		
    		boolean thresholdCrossed = false;
    		for (Field field :  schema.getEntity().getFields()) {
    			if (null != facetedFields) {
    				//if facetted set but field not included, then skip it
    				if (!ArrayUtils.contains(facetedFields, field.getOrdinal())) {
    					continue;
    				}
    			}
    			
    			//if ID or class attribute field, skip it
    			if (field.isId() ||  field.isClassAttribute()) {
    				continue;
    			}
    			
    			//track fields participating is dist calculation
    			if (includePassiveFields && null == passiveFields) {
    				if (null == activeFields) {
    					activeFields = new ArrayList<Integer>();
    				}
    				activeFields.add(field.getOrdinal());
    			}
    			
    			//extract fields
    			String firstAttr = "";
    			if (field.getOrdinal() < firstItems.length ){
    				firstAttr = firstItems[field.getOrdinal()];
    			} else {
    				throw new IOException("Invalid field ordinal. Looking for field " + field.getOrdinal() + 
    						" found "  + firstItems.length + " fields in the record:" + first);
    			}
    			
    			String secondAttr = "";
    			if (field.getOrdinal() < secondItems.length ){
    				secondAttr = secondItems[field.getOrdinal()];
    			}else {
    				throw new IOException("Invalid field ordinal. Looking for field " + field.getOrdinal() + 
    						" found "  + secondItems.length + " fields in the record:" + second);
    			}
    			String unit = field.getUnit();
    			
    			if (firstAttr.isEmpty() || secondAttr.isEmpty() ) {
    				//handle missing value
					//context.getCounter("Missing Data", "Field:" + field.getOrdinal()).increment(1);
					String missingValueHandler = schema.getMissingValueHandler();
    				if (missingValueHandler.equals("default")) {
       					//context.getCounter("Missing Data", "Distance Set at Max").increment(1);
       				    dist = 1.0;
    				} else if (missingValueHandler.equals("skip")) {
    					//context.getCounter("Missing Data", "FieldSkipped").increment(1);
    					continue;
    				} else {
    					//custom handler
    					
    				}
    			} else {
    				dist = 0;
	    			if (field.getDataType().equals(Field.DATA_TYPE_CATEGORICAL)) {
	    				//categorical
	    				dist = field.findDistance(firstAttr, secondAttr);
	    			} else if (field.getDataType().equals(Field.DATA_TYPE_INT)) {
	    				//int
	    				dist = numericDistance(field,  firstAttr,  secondAttr,  true, context);
	    			} else if (field.getDataType().equals(Field.DATA_TYPE_DOUBLE)) {
	    				//double
	    				dist =  numericDistance( field,  firstAttr,  secondAttr, false, context);
	    			} else if (field.getDataType().equals(Field.DATA_TYPE_TEXT)) { 
	    				//text
	    				dist = textSimStrategy.findDistance(firstAttr, secondAttr);	    				
	    			} else if (field.getDataType().equals(Field.DATA_TYPE_TIME_WINDOW)) {
	    				//time window
	    				dist = timeWindowDistance(field, firstAttr,  secondAttr, context);
	    			} else if (field.getDataType().equals(Field.DATA_TYPE_HOUR_WINDOW)) {
	    				//hour window
	    				dist = hourWindowDistance(field, firstAttr,  secondAttr, context);
	    			}   else if (field.getDataType().equals(Field.DATA_TYPE_LOCATION)) {
	    				//location
	    				dist = locationDistance(field, firstAttr,  secondAttr, context);
	    			} else if (field.getDataType().equals(Field.DATA_TYPE_GEO_LOCATION)) {
	    				//geo location
	    				dist = geoLocationDistance(field, firstAttr,  secondAttr, context);
	    			}  else if (field.getDataType().equals(Field.DATA_TYPE_EVENT)) {
	    				//event
	    				dist = eventDistance(field, firstAttr,  secondAttr, context);
	    			}
    			}
    			
    			//if threshold crossed for this attribute, skip the remaining attributes of the entity pair
    			thresholdCrossed = field.isDistanceThresholdCrossed(dist);
    			if (thresholdCrossed){
					//context.getCounter("Distance Data", "Attribute distance threshold filter").increment(1);
    				break;
    			}
    			
    			//aggregate attribute  distance for all entity attributes
				distStrategy.accumulate(dist, field);
    		}  
    		
    		//initialize passive fields
			if (includePassiveFields && null == passiveFields) {
				intializePassiveFieldOrdinal(activeFields, firstItems.length);
			}
			
    		netDist = thresholdCrossed?  distThreshold + 1  : distStrategy.getSimilarity();
    		return netDist;
        }
        
        /**
         * @param activeFields
         * @param numFields
         */
        private void intializePassiveFieldOrdinal(List<Integer> activeFields, int numFields) {
        	int len = numFields - activeFields.size();
        	if (len > 0) {
        		//all fields that are not active i.e not defined in schema
	        	passiveFields = new int[len];
	        	for (int i = 0,j=0; i < numFields; ++i) {
	        		if (!activeFields.contains(i) ) {
	        			passiveFields[j++] = i;
	        		}
	        	}
        	}
        }
        
        /**
         * generates output to emit
         * @return
         */
        private String createValueField(String first, String second) {
        	StringBuilder stBld = new StringBuilder();
        	
        	if (outputIdFirst) {
        		stBld.append(firstId).append(fieldDelim).append(secondId).append(fieldDelim);
        	}
        	
        	//include passive fields
        	if (null != passiveFields) {
        		for (int i :  passiveFields) {
        			stBld.append(firstItems[i]).append(fieldDelim);
        		}
        		for (int i :  passiveFields) {
        			stBld.append(secondItems[i]).append(fieldDelim);
        		}
        	}
        	
        	if (!outputIdFirst) {
        		stBld.append(firstId).append(fieldDelim).append(secondId).append(fieldDelim);
        	}
        	if (null != extraOutputFields) {
        		appendExtraField(firstItems, stBld);
        		appendExtraField(secondItems, stBld);
        	} else if (outputRecord) {
        		appendRecord(first, stBld);
        		appendRecord(second, stBld);
        	}
        	stBld.append(dist);
        	return stBld.toString();
        }

        /**
         * adds selected fields in the output
         * @param value
         * @param stBld
         */
        private void appendExtraField(String[] items, StringBuilder stBld) {
        	for (int extraOutputField : extraOutputFields) {
        		if (extraOutputField < items.length) {
        			stBld.append(fieldDelim).append(items[extraOutputField]);
        		}
        	}
        }
        
        /**
         * appends whole record
         * @param record
         * @param stBld
         */
        private void appendRecord(String record, StringBuilder stBld) {
        	stBld.append(fieldDelim).append(record);
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
					//context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
				}
			} else {
			}
        	return dist;
        }       
        
        /**
         * Distance as overlap between time ranges
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
    			//context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
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
        private double hourWindowDistance(Field field, String firstAttr, String secondAttr,Context context) {
        	double dist = 0;
    		try {
    			String[] subFields = firstAttr.split(subFieldDelim);
    			HourWindow firstTimeWindow = new HourWindow(subFields[0], subFields[1]);
    			subFields = secondAttr.split(subFieldDelim);
    			HourWindow secondTimeWindow = new HourWindow(subFields[0], subFields[1]);

    			dist = field.findDistance(firstTimeWindow, secondTimeWindow);
    		} catch (ParseException e) {
    			//context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
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
        private double geoLocationDistance(Field field, String firstAttr, String secondAttr,Context context) {
    		double dist = org.sifarish.util.Utility.getGeoDistance(firstAttr, secondAttr);
    		dist /= field.getMaxDistance();
    		dist = dist <= 1.0 ? dist : 1.0;
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
    			//context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
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
