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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.sifarish.util.Entity;
import org.sifarish.util.Field;
import org.sifarish.util.FieldMapping;
import org.sifarish.util.Utility;

/**
 * Similarity between two different entity types based distance measure of attributes
 * Attributes  from the two different entity types are linked through meta data. Meta data
 * for both entity types are defined in JSON
 * @author pranab
 */
public class DiffTypeSimilarity  extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Dirfferent type entity similarity MR";
        job.setJobName(jobName);
        
        job.setJarByClass(DiffTypeSimilarity.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DiffTypeSimilarity.SimilarityMapper.class);
        job.setReducerClass(DiffTypeSimilarity.SimilarityReducer.class);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setGroupingComparatorClass(IdPairGroupComprator.class);
        job.setPartitionerClass(IdPairPartitioner.class);
        
        Utility.setConfiguration(job.getConfiguration());

        int numReducer = job.getConfiguration().getInt("dts.num.reducer", -1);
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
        int exitCode = ToolRunner.run(new DiffTypeSimilarity(), args);
        System.exit(exitCode);
    }
    
    /**
     * @author pranab
     *
     */
    public static class SimilarityMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable keyHolder = new LongWritable();
        private Text valueHolder = new Text();
        private MixedTypeSchema schema;
        private int bucketCount;
        private long hash;
        private int idOrdinal;
        private String fieldDelimRegex;
        private boolean identifyWithFilePrefix;
        private Entity entity;
        private int filePrefixLength;
       
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	bucketCount = context.getConfiguration().getInt("bucket.count", 1000);
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
        	identifyWithFilePrefix = context.getConfiguration().getBoolean("identify.with.file.prefix", false);
        	if (identifyWithFilePrefix) {
        		filePrefixLength = Integer.parseInt(context.getConfiguration().get("file.prefix.length"));
        	}
        	
			Configuration conf = context.getConfiguration();
            String filePath = conf.get("schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, MixedTypeSchema.class);
       }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            
            if (null == entity) {
            	if (identifyWithFilePrefix) {
            		FileSplit fileInpSplit = (FileSplit)context.getInputSplit();
            		String filePrefix = fileInpSplit.getPath().getName().substring(0, filePrefixLength);
            		entity = schema.getEntityByFilePrefix(filePrefix);
            	} else {
            		entity = schema.getEntityBySize(items.length);
            	}
        		idOrdinal = entity.getIdField().getOrdinal();
            }

            if (null != entity){
        		hash = items[idOrdinal].hashCode() %  bucketCount;
        		hash = hash < 0 ?  -hash : hash;
            	if (entity.getType() == 0){
            		if (identifyWithFilePrefix) {
            			valueHolder.set ( "0," + value.toString());
            		} else {
            			valueHolder.set(value);
            		}
            		for (int i = 0; i < bucketCount; ++i) {
            			keyHolder.set((hash * bucketCount + i) * 10);
            			context.write(keyHolder, valueHolder);
            		}
            	} else {
            		if (identifyWithFilePrefix) {
            			valueHolder.set ( "1," + value.toString());
            		} else {
            			valueHolder.set(value);
            		}
            		for (int i = 0; i < bucketCount; ++i) {
            			keyHolder.set(((i * bucketCount + hash ) * 10) + 1);
            			context.write(keyHolder, valueHolder);
            		}
            	}
            } else {
            	
            }
        }        
    }   
    
    /**
     * @author pranab
     *
     */
    public static class SimilarityReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        private Text valueHolder = new Text();
        private MixedTypeSchema schema;
        private int firstTypeSize;
        private List<String> firstTypeValues = new ArrayList<String>();
        private int firstIdOrdinal;
        private int secondIdOrdinal;
        private String firstId;
        private String  secondId;
        private int firstClassAttrOrdinal = -1;
        private int secondClassAttrOrdinal = -1;
        private String firstClassAttr;
        private String secondClassAttr;
        private int sim;
        private List<Field> fields;
        private List<Field> targetFields;
        private int scale;
        private Map<Integer, MappedValue> mappedFields = new HashMap<Integer, MappedValue>();
        private static final int INVALID_ORDINAL = -1;
        private int srcCount;
        private int targetCount;
        private int simCount;
        private int simResultCnt;
        private boolean prntDetail;
        private DistanceStrategy distStrategy;
        private String fieldDelimRegex;
        private String fieldDelim;
        private DynamicAttrSimilarityStrategy textSimStrategy;
        private boolean outputVerbose;
        private boolean identifyWithFilePrefix;
        private boolean firstType;
        private String valueSt;
        private String[] items;
 
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	//load schema
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("schema.file.path");
            FileSystem dfs = FileSystem.get(conf);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, MixedTypeSchema.class);
        	
        	firstTypeSize = schema.getEntityByType(0).getFieldCount();
        	firstIdOrdinal = schema.getEntityByType(0).getIdField().getOrdinal();
        	secondIdOrdinal = schema.getEntityByType(1).getIdField().getOrdinal();
        	Field field = schema.getEntityByType(0).getClassAttributeField();
        	if (null != field) {
        		firstClassAttrOrdinal = field.getOrdinal();
        		secondClassAttrOrdinal = schema.getEntityByType(0).getClassAttributeField().getOrdinal();
        	}
        	
        	fields = schema.getEntityByType(0).getFields();
        	targetFields = schema.getEntityByType(1).getFields();
        	scale = context.getConfiguration().getInt("distance.scale", 1000);
        	distStrategy = schema.createDistanceStrategy(scale);
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
        	fieldDelim = context.getConfiguration().get("field.delim", ",");
        	textSimStrategy = schema.createTextSimilarityStrategy();
        	outputVerbose = context.getConfiguration().getBoolean("sim.output.verbose", true);
           	identifyWithFilePrefix = context.getConfiguration().getBoolean("identify.with.file.prefix", false);
                    	
        	System.out.println("firstTypeSize: " + firstTypeSize + " firstIdOrdinal:" +firstIdOrdinal + 
        			" secondIdOrdinal:" + secondIdOrdinal + " Source field count:" + fields.size() + 
        			" Target field count:" + targetFields.size());
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	firstTypeValues.clear();
        	srcCount = 0;
        	targetCount = 0;
        	simCount = 0;
        	StringBuilder stBld = new  StringBuilder();
        	
        	for (Text value : values){
        		String[] items = value.toString().split(fieldDelimRegex);
        		
        		if ( identifyWithFilePrefix) {
        			firstType = value.toString().startsWith("0");
        			valueSt = value.toString().substring(2, value.toString().length());
        		} else {
        			firstType = items.length == firstTypeSize;
        			valueSt = value.toString();
        		}
        		
        		if (firstType){
        			firstTypeValues.add(valueSt);
        			++srcCount;
        		} else {
    				String second = valueSt;
    				items = second.split(fieldDelimRegex);
    				secondId = items[secondIdOrdinal];
    				if (secondClassAttrOrdinal >= 0) {
    					secondClassAttr = items[secondClassAttrOrdinal];
    				}
        			for (String first : firstTypeValues){
        				//prntDetail =  ++simResultCnt % 10000 == 0;
        				sim = findSimilarity(first, second, context);
        				items = first.split(fieldDelimRegex);
        				firstId = items[firstIdOrdinal];
           				if (firstClassAttrOrdinal >= 0) {
           					firstClassAttr = items[firstClassAttrOrdinal];
        				}
            				
        				if (outputVerbose) {
        					if (firstClassAttrOrdinal > 0) {
        						stBld.append(firstId).append(fieldDelim).append(firstClassAttr).append(fieldDelim).append(secondClassAttr).
        							append(second).append(fieldDelim).append(sim);
        					} else {
        						stBld.append(firstId).append(fieldDelim).append(second).append(fieldDelim).append(sim);
        					}
        					valueHolder.set(stBld.toString());
        				} else {
        					if (firstClassAttrOrdinal > 0) {
        						stBld.append(firstId).append(fieldDelim).append(secondId).append(fieldDelim).append(firstClassAttr).
        							append(fieldDelim).append(secondClassAttr).append(sim);
        					} else {
        						stBld.append(firstId).append(fieldDelim).append(secondId).append(fieldDelim).append(sim);
        					}
        					valueHolder.set(stBld.toString());
        				}
        				context.write(NullWritable.get(), valueHolder);
        				++simCount;
        			}
        			++targetCount;
        		}
        	}
			//context.getCounter("Data", "Source Count").increment(srcCount);
			//context.getCounter("Data", "Target Count").increment(targetCount);
			//context.getCounter("Data", "Similarity Count").increment(simCount);
        	
        }
        
    	/**
    	 * Gets distance between two entities
    	 * @param source
    	 * @param target
    	 * @param context
    	 * @return
    	 * @throws IOException 
    	 */
    	private int findSimilarity(String source, String target, Context context) throws IOException {
    		int sim = 0;
    		mapFields(source, context);
    		String[] trgItems = target.split(fieldDelimRegex);
    		
    		double dist = 0;
			//context.getCounter("Data", "Target Field Count").increment(targetFields.size());
			if (prntDetail){
				System.out.println("target record: " + trgItems[0]);
			}
			
			distStrategy.initialize();
    		for (Field field : targetFields) {
    			dist = 0;
    			Integer ordinal = field.getOrdinal();
				String trgItem = trgItems[ordinal];
				boolean skipAttr = false;
				if (prntDetail){
					System.out.println("ordinal: " + ordinal +  " target:" + trgItem);
				}
				
				MappedValue mappedValueObj = mappedFields.get(ordinal);
				if (null == mappedValueObj){
    				//non mapped passive attributes
					continue;
				}
				
    			List<String> mappedValues = mappedValueObj.getValues();
    			Field srcField = mappedValueObj.getField();
				if (!trgItem.isEmpty()) {
    				if (field.getDataType().equals("categorical")) {
    					if (!mappedValues.isEmpty()) {
	    					double thisDist;
	    					dist = 1.0;
	    					for (String mappedValue : mappedValues) {
	    						thisDist = schema.findCattegoricalDistance(mappedValue, trgItem, ordinal);
	    						if (thisDist < dist) {
	    							dist = thisDist;
	    						}
		    					if (prntDetail){
		    						System.out.println("dist calculation: ordinal: " + ordinal + " src:" + mappedValue + " target:" + trgItem + 
		    								" dist:" + dist);
		    					}
	    					}
	    					
    						//context.getCounter("Data", "Dist Calculated").increment(1);
    					} else {
    						//missing source
    						if (schema.getMissingValueHandler().equals("default")){
    							dist = getDistForMissingSrc(field, trgItem);
    						} else {
    							skipAttr = true;
    						}
    						//context.getCounter("Data", "Missing Source").increment(1);
    					}
    				} else if (field.getDataType().equals("int")) {
    					if (!mappedValues.isEmpty()) {
	    					int trgItemInt = Integer.parseInt(trgItem);
    						int srcItemInt = getAverageMappedValue(mappedValues);
    						dist = getDistForNumeric(srcField, srcItemInt, field, trgItemInt);
    					} else {
    						//missing source
    						if (schema.getMissingValueHandler().equals("default")){
    							dist = getDistForMissingSrc(field, trgItem);
    						} else {
    							skipAttr = true;
    						}
    					}
    				} else if (field.getDataType().equals("text")) {
       					if (!mappedValues.isEmpty()) {
       						String trgItemTxt = trgItem;
       						String srcItemTxt = mappedValues.get(0);
       						dist = textSimStrategy.findDistance(trgItemTxt, srcItemTxt);
    					} else {
    						//missing source
    						if (schema.getMissingValueHandler().equals("default")){
    							dist = getDistForMissingSrc(field, trgItem);
    						} else {
    							skipAttr = true;
    						}
    					}
    				} else if (field.getDataType().equals("location")) {
      					if (!mappedValues.isEmpty()) {
       						String trgItemTxt = trgItem;
       						String srcItemTxt = mappedValues.get(0);
       						dist = getDistForLocation(trgItemTxt, srcItemTxt, field);
    					} else {
    						//missing source
    						skipAttr = true;
    					}  						
    				}
				} else {
					//missing target value
					if (schema.getMissingValueHandler().equals("default")){
						//context.getCounter("Data", "Missing Target").increment(1);
						dist = getDistForMissingTrg(field, mappedValues);
					} else {
						skipAttr = true;
					}
				}
    			
				if (!skipAttr) {
					distStrategy.accumulate(dist, field);
				}
    		}
    		sim = distStrategy.getSimilarity();
    		return sim;
    	}
    	
    	/**
    	 * Gets distance between numerial values
    	 * @param srcField
    	 * @param srcVal
    	 * @param trgField
    	 * @param trgVal
    	 * @return
    	 */
    	private double getDistForNumeric(Field srcField, int srcVal, Field trgField, int trgVal){
    		double dist = 0;
    		boolean linear = false;
    		String distFun = srcField.getNumDistFunction();
    		
    		if (distFun.equals("equalSoft")) {
    			linear = true;
    		} else if (distFun.equals("equalHard")) { 
    			dist = srcVal == trgVal ? 0 : 1;
    		} else if (distFun.equals("minSoft")) {
    			if (trgVal >= srcVal) {
    				dist = 0;
    			} else {
    				linear = true;
    			}
    		} else if (distFun.equals("minHard")) {
    			dist = trgVal >= srcVal ? 0 : 1;
    		} else if (distFun.equals("maxSoft")) {
    			if (trgVal <= srcVal) {
    				dist = 0;
    			} else {
    				linear = true;
    			}
    		} else if (distFun.equals("maxHard")) {
    			dist = trgVal <= srcVal ? 0 : 1;
    		}
    		
    		if (linear) {
    			if (trgField.getMax() > trgField.getMin()) {
    				dist = ((double)(srcVal - trgVal)) / (trgField.getMax() - trgField.getMin());
       			} else {
					int max = srcVal > trgVal ? srcVal : trgVal;
					double diff = ((double)(srcVal - trgVal)) / max;
					if (diff < 0) {
						diff = - diff;
					}
					dist = diff > schema.getNumericDiffThreshold() ? 1.0 : 0.0;
       				
       			}
				if (dist < 0) {
					dist = -dist;
				}
   			}
    		
    		return dist;
    	}
    	
    	/**
    	 * Gets distance between geo location values 
    	 * @param trgItemTxt
    	 * @param srcItemTxt
    	 * @param field
    	 * @return
    	 */
    	private double getDistForLocation(String trgItemTxt, String srcItemTxt,  Field field ) {
    		double dist = org.sifarish.util.Utility.getGeoDistance(trgItemTxt, srcItemTxt);
    		dist /= field.getMaxDistance();
    		dist = dist <= 1.0 ? dist : 1.0;
    		return dist;
    	}
    	
    	/**
    	 * gets distance for missing source field
    	 * @param trgField
    	 * @param trgVal
    	 * @return
    	 */
    	private double getDistForMissingSrc(Field trgField, String trgVal){
    		double dist = 0;
			if (trgField.getDataType().equals("categorical") || trgField.getDataType().equals("text")) {
				dist = 0;
			} else if (trgField.getDataType().equals("int")) {
				int trgValInt = Integer.parseInt(trgVal);
				int max = trgField.getMax();
				int min = trgField.getMin();
				if (max > trgField.getMin()) {
					double upper = ((double)(max - trgValInt)) / (max - min);
					double lower = ((double)(trgValInt - min)) / (max - min);
					dist = upper > lower ? upper : lower;
				} else {
					dist = 1;
				}
			}
    		return dist;
    	}
    	
    	/** Gets distance when source attribute is missing
    	 * @param trgField
    	 * @param mappedValues
    	 * @return
    	 */
    	private double getDistForMissingTrg(Field trgField, List<String> mappedValues){
    		double dist = 0;
			if (trgField.getDataType().equals("categorical") || trgField.getDataType().equals("text")) {
				dist = 1;
			}  else if (trgField.getDataType().equals("int")) {
				int srcValInt = getAverageMappedValue(mappedValues);
				int max = trgField.getMax();
				int min = trgField.getMin();
				if (max > min) {
					double upper = ((double)(max - srcValInt)) / (max - min);
					double lower = ((double)(srcValInt - min)) / (max - min);
					dist = upper > lower ? upper : lower;
				} else {
					dist = 1;
				}
			}

    		return dist;
    	}
    	
    	/**
    	 * Gets values for the mapped attributes in src mapped from the target
    	 * @param source
    	 * @param context
    	 */
    	private void mapFields(String source, Context context){
	    	mappedFields.clear();
			String[] srcItems = source.split(fieldDelimRegex);
			
			if (prntDetail){
				System.out.println("src record: " + srcItems[0]);
			}
			for (Field field : fields) {
				List<FieldMapping> mappings = field.getMappings();
				if (null != mappings){
					for (FieldMapping fldMapping : mappings) {
						int matchingOrdinal = fldMapping.getMatchingOrdinal();
						if (-1 == matchingOrdinal) {
							continue;
						}
						
						MappedValue mappedValue = mappedFields.get(matchingOrdinal);
						if (null == mappedValue){
							mappedValue = new MappedValue();
							mappedValue.setField(field);
							mappedFields.put(matchingOrdinal, mappedValue);
						}
						List<String> mappedValues = mappedValue.getValues();
						
						String value = srcItems[field.getOrdinal()];
						if (prntDetail){
							System.out.println("src value: " + value);
						}
						
						List<FieldMapping.ValueMapping> valueMappings = fldMapping.getValueMappings();
						if (null != valueMappings) {
							for (FieldMapping.ValueMapping valMapping : valueMappings) {
								if (field.getDataType().equals("categorical")) {
									//store mapped values
									if (valMapping.getThisValue().equals(value)) {
										mappedValues.add(valMapping.getThatValue());
			    						//context.getCounter("Data", "Mapped Value").increment(1);
			    						if (prntDetail){
			    							System.out.println("mapped: " + value + "  " + valMapping.getThatValue() + 
			    									" matching ordinal:" + matchingOrdinal);
			    						}
										break;
									}
								} else if (field.getDataType().equals("int")) {
									int valueInt = Integer.parseInt(value);
									int[] range = valMapping.getThisValueRange();
									if (null != range) {
										if (valueInt >= range[0] && valueInt <= range[1]) {
											mappedValues.add(valMapping.getThatValue());
											break;
										}
									}
								}
							}
						} else {
    						if (prntDetail){
    							System.out.println("non mapped: " + value + " matching ordinal:" + matchingOrdinal);
    						}
							if (!value.isEmpty()) {
								mappedValues.add(value);
							}
						}
					}
				} 
			}
	    }
    	
        /**
         * @param mappedValues
         * @return
         */
        private int getAverageMappedValue(List<String> mappedValues){
    		int sum = 0;
    		int count = 0;
    		for (String mappedValue : mappedValues) {
    			sum += Integer.parseInt(mappedValue);
    			++count;
    		}    	
    		int fItemInt = sum / count;
    		return fItemInt;
        }
    }  

    /**
     * @author pranab
     *
     */
    public static class IdPairPartitioner extends Partitioner<LongWritable, Text> {
	     @Override
	     public int getPartition(LongWritable key, Text value, int numPartitions) {
	    	 //consider only base part of  key
		     int keyVal = (int)(key.get() / 10);
		     return keyVal % numPartitions;
	     }
    
    }

    /**
     * @author pranab
     *
     */
    public static class IdPairGroupComprator extends WritableComparator {
    	private static final int KEY_EXTENSION_SCALE = 10;
    	protected IdPairGroupComprator() {
    		super(LongWritable.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		//consider only the base part of the key
    		Long t1 = ((LongWritable)w1).get() / KEY_EXTENSION_SCALE;
    		Long t2 = ((LongWritable)w2).get() / KEY_EXTENSION_SCALE;
    		
    		int comp = t1.compareTo(t2);
    		return comp;
    	}
     }
    
    /**
     * @author pranab
     *
     */
    public static class MappedValue {
    	private List<String> values = new ArrayList<String>();
    	private Field field;
    	
		public List<String> getValues() {
			return values;
		}
		public void setValues(List<String> values) {
			this.values = values;
		}
		public Field getField() {
			return field;
		}
		public void setField(Field field) {
			this.field = field;
		}
    	
    }
    
}
