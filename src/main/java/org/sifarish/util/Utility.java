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

package org.sifarish.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Utility
 * @author pranab
 *
 */
public class Utility {
	private static final String S3_PREFIX = "s3n:";
	private static Pattern s3pattern = Pattern.compile("s3n:/+([^/]+)/+(.*)");
    static AmazonS3 s3 = null;
	static {
		try {	
			s3 = new AmazonS3Client(new PropertiesCredentials(Utility.class.getResourceAsStream("AwsCredentials.properties")));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

    public static void setConfiguration(Configuration conf) throws Exception{
        String confFilePath = conf.get("conf.path");
        if (null != confFilePath){
        	InputStream is = null;
        	if (confFilePath.startsWith(S3_PREFIX)) { 
        		Matcher matcher = s3pattern.matcher(confFilePath);
        		matcher.matches();
        		String bucket = matcher.group(1);
        		String key = matcher.group(2);
        		S3Object object = s3.getObject(new GetObjectRequest(bucket, key));
                is = object.getObjectContent();
        	}
        	else {
        		is = new FileInputStream(confFilePath);
        	}
            Properties configProps = new Properties();
            configProps.load(is);

            for (Object key : configProps.keySet()){
                String keySt = key.toString();
                conf.set(keySt, configProps.getProperty(keySt));
            }
        }
    }

}
