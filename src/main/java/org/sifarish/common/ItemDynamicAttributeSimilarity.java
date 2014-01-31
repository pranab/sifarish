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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.IntPair;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.sifarish.feature.DynamicAttrSimilarityStrategy;

/**
 * Mapreduce for finding similarities between items with dynamic set of attributes. For example,  products 
 * where the atrributes   are users who have purchased it or documents where the attributes are terms in
 * the documents.
 * 
 * @author pranab
 *
 */
public class ItemDynamicAttributeSimilarity  extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception   {
        Job job = new Job(getConf());
        String jobName = "Item with dynamic attribute  similarity MR";
        job.setJobName(jobName);
        
        job.setJarByClass(ItemDynamicAttributeSimilarity.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ItemDynamicAttributeSimilarity.SimilarityMapper.class);
        job.setReducerClass(ItemDynamicAttributeSimilarity.SimilarityReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
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
     * @author pranab
     *
     */
    public static class SimilarityMapper extends Mapper<LongWritable, Text, Tuple, Text> {
        private int bucketCount;
        private int hash;
        private String fieldDelimRegex;
        private Integer hashPair;
        private Integer one = 1;
        private Integer zero = 0;
        private String itemID;
        private Tuple keyHolder = new Tuple();
        private Text valueHolder = new Text();
        private int hashPairMult;
        private int hashCode;
        private int partitonFieldOrdinal;
        private static final Logger LOG = Logger.getLogger(ItemDynamicAttributeSimilarity.SimilarityMapper.class);
    	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
            if (conf.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             	System.out.println("in debug mode");
            }
        	bucketCount = conf.getInt("bucket.count", 10);
        	fieldDelimRegex = conf.get("field.delim.regex", "\\[\\]");
        	hashPairMult = conf.getInt("hash.pair.multiplier", 1000);
        	partitonFieldOrdinal = conf.getInt("paritioning.field.ordinal", -1);
        }    
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	//first token is entity ID and the rest list attributes
        	String[] items  =  value.toString().split(fieldDelimRegex);
        	itemID  =  items[0];
        	hashCode = itemID.hashCode();
        	if (hashCode < 0) {
        		hashCode = - hashCode;
        	}
    		hash = (hashCode %  bucketCount) / 2 ;
    		String partition = partitonFieldOrdinal >= 0 ? items[partitonFieldOrdinal] :  "none";
    		
    		for (int i = 0; i < bucketCount;  ++i) {
    			keyHolder.initialize();
    			if (i < hash){
       				hashPair = hash * hashPairMult +  i;
       				keyHolder.add(partition, hashPair, zero);
       				valueHolder.set("0" + value.toString());
       	   		 } else {
    				hashPair =  i * hashPairMult  +  hash;
       				keyHolder.add(partition, hashPair, one);
       				valueHolder.set("1" + value.toString());
    			} 
    			//System.out.println("mapper hashPair: " + hashPair);
   	   			context.write(keyHolder, valueHolder);
    		}
        }
             
    }
    
    /**
     * @author pranab
     *
     */
    public static class SimilarityReducer extends Reducer<Tuple, Text, NullWritable, Text> {
        private Text valueHolder = new Text();
        private String fieldDelim;
    	private String fieldDelimRegex;
        private int delimLength;
        private int hashPairMult;
        private List<String[]> valueList = new ArrayList<String[]>();
        private DynamicAttrSimilarityStrategy simStrategy;
        private int scale;
        private boolean outputCorrelation;
        private int partitonFieldOrdinal;
        private int intLength;
        private int minIntLength;
        private boolean addMatchingContext;
        private int semanticScale;
       	private StringBuilder stBld = new StringBuilder();
        private static final Logger LOG = Logger.getLogger(ItemDynamicAttributeSimilarity.SimilarityReducer.class);
               
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
            if (conf.getBoolean("debug.on", false)) {
             	LOG.setLevel(Level.DEBUG);
             	System.out.println("in debug mode");
            }
        	
        	fieldDelim = conf.get("field.delim", "[]");
        	fieldDelimRegex = conf.get("field.delim.regex", "\\[\\]");
        	delimLength =  fieldDelim.length();
        	hashPairMult = conf.getInt("hash.pair.multiplier", 1000);
        	String simAlgorithm = conf.get("similarity.algorithm", "cosine");
        	
        	//semantic matching
        	Map<String, Object> params = new HashMap<String, Object>();
        	params.put("matcherClass", conf.get("semantic.matcher.class"));
        	params.put("topMatchCount", conf.getInt("semantic.top.match.count", 5));
        	params.put("semanticScale", conf.getInt("semantic.match.scale", 10));
        	params.put("config", conf);
        	loadSemanticMatcherParams( conf,  params); 
        	
        	LOG.debug("simAlgorithm:" + simAlgorithm + " matcherClass: "  + conf.get("semantic.matcher.class"));
        	
        	//similarity matching algorithm
        	params.put("srcNonMatchingTermWeight", conf.get("jaccard.srcNonMatchingTermWeight"));
        	params.put("trgNonMatchingTermWeight", conf.get("jaccard.trgNonMatchingTermWeight"));
        	simStrategy = DynamicAttrSimilarityStrategy.createSimilarityStrategy(simAlgorithm, params);
        	
        	simStrategy.setFieldDelimRegex(fieldDelimRegex);
        	boolean booleanVec = conf.getBoolean("vec.type.boolean", true);
        	boolean semanticVec = conf.getBoolean("vec.type.semantic", false);
        	LOG.debug("booleanVec:" + booleanVec + " semanticVec:" + semanticVec);
        	addMatchingContext = conf.getBoolean("add.semantic.matching.context", false);
        	
        	//vector type
        	if (booleanVec){
        		simStrategy.setBooleanVec(booleanVec);
        	}
        	if (semanticVec){
        		simStrategy.setSemanticVec(semanticVec);
        	}
        	if (!booleanVec && !semanticVec) {
            	boolean countIncluded = conf.getBoolean("vec.count.included", true);
        		simStrategy.setCountIncluded(countIncluded);
        	}
        	
           	scale = conf.getInt("distance.scale", 1000);
           	outputCorrelation = conf.getBoolean("output.correlation", false);
           	partitonFieldOrdinal = conf.getInt("paritioning.field.ordinal", -1);
           	minIntLength =  conf.getInt("min.intersection.length", 2);
           	LOG.debug("outputCorrelation:" + outputCorrelation + " partitonFieldOrdinal:" + partitonFieldOrdinal +
           			" minIntLength:" + minIntLength);
          }    
        
        /**
         * @param conf
         * @param params
         */
        private void loadSemanticMatcherParams(Configuration conf, Map<String, Object> params ) {
        	String semParams = conf.get("semantic.matcher.params");
        	if (!StringUtils.isBlank(semParams)) {
	        	String[] semanticParams = semParams.split(",");
	        	for (String semanticParam :  semanticParams) {
	        		params.put(semanticParam, conf.get(semanticParam));
	        	}
        	}
        	
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	double dist = 0;
        	intLength = -1;
        	
        	valueList.clear();
        	int firstPart = key.getInt(1);
        	//System.out.println("hashPair: " + firstPart);
        	if (firstPart / hashPairMult == firstPart % hashPairMult){
        		//same hash bucket
    			context.getCounter("Reducer", "Same  Bucket Count").increment(1);
    			//System.out.println("**same bucket");
    			
	        	for (Text value : values){
	        		String valSt = value.toString();
        			String[] parts = splitKey(valSt.substring(1));
        			valueList.add(parts);
	        	}   
	        	
	        	for (int i = 0;  i < valueList.size();  ++i){
	        		String[] firstParts = valueList.get(i);
	        		for (int j = i+1;  j < valueList.size();  ++j) {
		        		String[] secondParts = valueList.get(j);
		        		//process 2 user vectors
	        			dist = ( 1.0 - simStrategy.findDistance(firstParts[1], secondParts[1]))  * scale;
	        			dist = dist < 0.0 ? 0.0 : dist;
		        		
    					intLength = simStrategy.getIntersectionLength();
    					if( intLength >= minIntLength || simStrategy.isSemanticVec()) {
	        				if (outputCorrelation) {
	        					dist = scale - dist;
	            				//2 items IDs followed by distance and intersection length
	        					stBld.append(firstParts[0]).append(fieldDelim).append(secondParts[0]).append(fieldDelim).
	        						append( (int)dist).append(fieldDelim).append(intLength);
	        				} else {
	            				//2 items IDs followed by distance
	    	   					stBld.append(firstParts[0]).append(fieldDelim).append(secondParts[0]).append(fieldDelim).
	    	   						append( (int)dist);
	        				}
	        				
	        				//if there any matching context data
	        				if(addMatchingContext) {
	        					appendMatchingContexts(stBld);
	        				}
	
	        				valueHolder.set(stBld.toString());
		   	    			context.getCounter("Reducer", "Emit").increment(1);
		   					context.write(NullWritable.get(), valueHolder);
		        			stBld.delete(0, stBld.length());
	    				} else {
		   	    			context.getCounter("Correlation Intersection", "Below threshold").increment(1);
	    				} //if int length
	        		}//for
	        	}//for
        	} else {
        		//different hash bucket
    			context.getCounter("Reducer", "Diff Bucket Count").increment(1);
    			//System.out.println("**diff  bucket");
	        	for (Text value : values){
	        		String valSt = value.toString();
	        		if (valSt.startsWith("0")) {
	        			String[] parts = splitKey(valSt.substring(1));
	        			valueList.add(parts);
	        		} else {
	        			String[] parts = splitKey(valSt.substring(1));
	        			
	        			//match with all items of first set
	        			for (String[] firstParts : valueList) {
	        				//process 2 entity vectors
	        				dist = (1.0 - simStrategy.findDistance(firstParts[1], parts[1])) * scale;
	        				dist = dist < 0.0 ? 0.0 : dist;
	        				LOG.debug("dist:" + dist);
	        				
        					intLength = simStrategy.getIntersectionLength();
        					if( intLength >= minIntLength || simStrategy.isSemanticVec()) {
		        				if (outputCorrelation) {
		        					dist = scale - dist;
		            				//2 items IDs followed by distance and intersection l;ength
		           					stBld.append(firstParts[0]).append(fieldDelim).append(parts[0]).append(fieldDelim).
		           						append( (int)dist).append(fieldDelim).append(intLength);
	 	        				} else {
		            				//2 items IDs followed by distance
		    	   					stBld.append(firstParts[0]).append(fieldDelim).append(parts[0]).append(fieldDelim).
	    	   						append( (int)dist);
		        				}
	
		        				//if there any matching context data
		        				if(addMatchingContext) {
		        					appendMatchingContexts(stBld);
		        				}
	
		        				valueHolder.set(stBld.toString());
			   	    			context.getCounter("Reducer", "Emit").increment(1);
			   					context.write(NullWritable.get(), valueHolder);
			        			stBld.delete(0, stBld.length());
		        			} else {
			   	    			context.getCounter("Correlation Intersection", "Below threshold").increment(1);
		        			}//if int length
	        			}//for
	        		}//if
	        	}//for
        	}//if
       	
        }
        
        /**
         * @param stBld
         */
        private void appendMatchingContexts(StringBuilder stBld) {
			String[]   matchingContexts = simStrategy.getMatchingContexts();
			if (null  !=  matchingContexts) {
				for (String matchingContext :  matchingContexts) {
					stBld.append(fieldDelim).append(matchingContext);
				}
			}
        }
        
        
        /**
         * @param val
         * @return
         */
        private String[]   splitKey(String val) {
        	String[] parts = new String[2];
        	int pos = val.indexOf(fieldDelim);
        	
        	//entity ID
        	parts[0] = val.substring(0, pos);
        	
        	//list of attributes
        	if (partitonFieldOrdinal >= 0) {
        		//partitioning field in the second
        		val = val.substring( pos + delimLength);
        		pos = val.indexOf(fieldDelim);
        		parts[1] = val.substring( pos + delimLength);
        	} else {
        		//attributes from the second field onwards
        		parts[1] = val.substring( pos + delimLength);
        	}
        	return parts;
        }
        
    }
    
    /**
     * @author pranab
     *
     */
    public static class IdPairPartitioner extends Partitioner<Tuple, Text> {
	     @Override
	     public int getPartition(Tuple key, Text value, int numPartitions) {
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
    		super(Tuple.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		//consider only the base part of the key
    		Tuple t1 = (Tuple)w1;
    		Tuple t2 = (Tuple)w2;
    		
    		int comp =t1.compareToBase(t2);
    		return comp;
    	}
     }
  
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ItemDynamicAttributeSimilarity(), args);
        System.exit(exitCode);
    }

}
