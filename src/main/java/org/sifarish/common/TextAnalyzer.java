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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.sifarish.util.Utility;

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
        
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        Utility.setConfiguration(job.getConfiguration());
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    public static class AnalyzerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private Text valueHolder = new Text();
        private String fieldDelim;
        private String fieldDelimRegex;
        private Set<Integer> textFieldOrdinals = new HashSet<Integer>();
        private Analyzer analyzer;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", "[]");
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
        	String textFields = context.getConfiguration().get("text.field.ordinals", "");
            String[] items  =  textFields.toString().split(",");
            for (int i = 0; i < items.length; ++i){
            	textFieldOrdinals.add(Integer.parseInt(items[i]));
            }
            analyzer = new StandardAnalyzer(Version.LUCENE_35);
       }

        protected void cleanup(Context context) throws IOException, InterruptedException {
        	
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(fieldDelimRegex);
            StringBuilder stBld = new StringBuilder();
            
            for (int i = 0;i < items.length; ++i) {
            	String item = items[i];
            	if (textFieldOrdinals.contains(i)) {
            		//if text field analyze
            		item = tokenize(item);
            	}
            	
            	if (i == 0){
            		stBld.append(item);
            	} else {
            		stBld.append(fieldDelim).append(item);
            	}
            }
            valueHolder.set(stBld.toString());
			context.write(NullWritable.get(), valueHolder);
       }     
        

        private String tokenize(String text) throws IOException {
            TokenStream stream = analyzer.tokenStream("contents", new StringReader(text));
            StringBuilder stBld = new StringBuilder();

            CharTermAttribute termAttribute = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
            while (stream.incrementToken()) {
        		String token = termAttribute.toString();
        		stBld.append(token).append(" ");
        	} 
        	
        	return stBld.toString();
        }
    }    
    
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TextAnalyzer(), args);
        System.exit(exitCode);
    }
	

}
