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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.morfologik.MorfologikAnalyzer;
import org.codehaus.jackson.map.ObjectMapper;
import org.sifarish.feature.SingleTypeSchema;
import org.sifarish.util.Field;
import org.sifarish.util.FieldExtractor;
import org.sifarish.util.Utility;

/**
 * @author pranab
 *
 */
public class TextAnalyzer extends Configured implements Tool{
	
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Text analyzer MR";
        job.setJobName(jobName);
        
        job.setJarByClass(TextAnalyzer.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TextAnalyzer.AnalyzerMapper.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        Utility.setConfiguration(job.getConfiguration());
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    /**
     * @author pranab
     *
     */
    public static class AnalyzerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private Text valueHolder = new Text();
        private String fieldDelim;
        private String fieldDelimRegex;
        private boolean consolidateFields;
        private Set<Integer> textFieldOrdinals = new HashSet<Integer>();
        private Analyzer analyzer;
        private List<String> itemList = new ArrayList<String>();
        private SingleTypeSchema schema;
        private Map<Integer, String> extrtactedFields = new HashMap<Integer, String>();
        private Set<Integer> retainedFieldOrdinals = new HashSet<Integer>();
        	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim", "[]");
        	fieldDelimRegex = config.get("field.delim.regex", "\\[\\]");
        	consolidateFields = config.getBoolean("tea.consolidate.field", false);
        	String textFields = config.get("tea.text.field.ordinals", "");
            String[] items  =  textFields.toString().split(",");
            for (int i = 0; i < items.length; ++i){
            	textFieldOrdinals.add(Integer.parseInt(items[i]));
            }
            
            //language specific analyzer
            String lang = config.get("tea.text.language", "en");
            createAnalyzer(lang);
            
            //load schema
            String filePath = config.get("tea.raw.schema.file.path");
            FileSystem dfs = FileSystem.get(config);
            Path src = new Path(filePath);
            FSDataInputStream fs = dfs.open(src);
            ObjectMapper mapper = new ObjectMapper();
            schema = mapper.readValue(fs, SingleTypeSchema.class);

    		for (Field field  :  schema.getEntity().getFields()){
    			retainedFieldOrdinals.add(field.getOrdinal());
    		}
           
       }
        
        /**
         * creates language specific analyzers
         * @param lang
         */
        private void createAnalyzer(String lang) {
        	if (lang.equals("en")) {
        		analyzer = new EnglishAnalyzer(Version.LUCENE_44);
        	} else if (lang.equals("de")) {
        		analyzer = new GermanAnalyzer(Version.LUCENE_44);
        	} else if (lang.equals("es")) {
        		analyzer = new SpanishAnalyzer(Version.LUCENE_44);
    		} else if (lang.equals("fr")) {
        		analyzer = new FrenchAnalyzer(Version.LUCENE_44);
    		} else if (lang.equals("it")) {
        		analyzer = new ItalianAnalyzer(Version.LUCENE_44);
    		} else if (lang.equals("br")) {
        		analyzer = new BrazilianAnalyzer(Version.LUCENE_44);
    		} else if (lang.equals("ru")) {
        		analyzer = new RussianAnalyzer(Version.LUCENE_44);
    		} else if (lang.equals("pl")) {
        		analyzer = new MorfologikAnalyzer(Version.LUCENE_44);
    		} else {
    			throw new IllegalArgumentException("unsupported language:" + lang);
    		} 
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
            StringBuilder stBld = new StringBuilder();
            StringBuilder consFields = new StringBuilder();
            itemList.clear();
            extrtactedFields.clear();
            
            for (int i = 0;i < items.length; ++i) {
            	String item = items[i];
            	
            	if (textFieldOrdinals.contains(i)) {
            		//extract fields
            		findExtractedFields(i, item);
            		
            		//if text field analyze
            		item = tokenize(item);
            		if (consolidateFields){
            			consFields.append(item);
            			item = null;
            		}
            	}
            	
            	if (null != item) {
            		//only if retained
            		if (retainedFieldOrdinals.contains(i)) {
            			itemList.add(item);
            		}
            	}
            }
            
            //consolidated field at end
            if (consolidateFields) {
            	itemList.add(consFields.toString());
            }
            
            //add extracted fields
            for (int e = 0; e < extrtactedFields.size(); ++ e) {
            	String extField = extrtactedFields.get(e);
            	itemList.add(extField);
            }
            
            //build value string
            boolean first = true;
            for (String item : itemList){
            	if (first){
            		stBld.append(item);
            		first = false;
            	} else {
            		stBld.append(fieldDelim).append(item);
            	}
            }
            
            valueHolder.set(stBld.toString());
			context.write(NullWritable.get(), valueHolder);
       }     
        
        /**
         * @param ordinal
         * @param data
         */
        private void findExtractedFields(int ordinal, String data) {
        	List<FieldExtractor> extractors = schema.getEntity().getExtractorsForField(ordinal);
        	for (FieldExtractor extractor : extractors) {
        		String extField = extrtactedFields.get(extractor.getOrdinal());
        		if (null == extField || extField.isEmpty()) {
	        		String match = extractor. findMatch(data);
	        		if (null == match){
	        			match = "";
	        		}
        			extrtactedFields.put(extractor.getOrdinal(), match);
        		}
        	}
        }

        /**
         * @param text
         * @return
         * @throws IOException
         */
        private String tokenize(String text) throws IOException {
            TokenStream stream = analyzer.tokenStream("contents", new StringReader(text));
            StringBuilder stBld = new StringBuilder();

            stream.reset();
            CharTermAttribute termAttribute = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
            while (stream.incrementToken()) {
        		String token = termAttribute.toString();
        		stBld.append(token).append(" ");
        	} 
        	stream.end();
        	stream.close();
        	return stBld.toString();
        }
    }    
    
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TextAnalyzer(), args);
        System.exit(exitCode);
    }
	

}
