package org.sifarish.util;

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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldExtractor {
	private String name;
	private int ordinal;
	private int srcOrdinal;
	private String pattern;
	private Pattern patternObj;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getOrdinal() {
		return ordinal;
	}
	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}
	public int getSrcOrdinal() {
		return srcOrdinal;
	}
	public void setSrcOrdinal(int srcOrdinal) {
		this.srcOrdinal = srcOrdinal;
	}
	public String getPattern() {
		return pattern;
	}
	public void setPattern(String pattern) {
		this.pattern = pattern;
		patternObj = Pattern.compile(pattern);
	}
	public Pattern getPatternObj() {
		return patternObj;
	}
	
	public boolean allSrcFields() {
		return ordinal == -1;
	}
	
	public String findMatch(String data){
		String match = null;
		Matcher matcher = patternObj.matcher(data);
		if (matcher.find() && matcher.groupCount() >= 1) {
			match = matcher.group(1);
		}
		
		return match;
		
	}

}
