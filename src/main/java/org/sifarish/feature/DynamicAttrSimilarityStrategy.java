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

import java.util.Map;

/**
 * Entity attributes are variable and dynamic
 * @author pranab
 *
 */
public abstract class DynamicAttrSimilarityStrategy {
	
	protected String fieldDelimRegex = "\\s+";
	protected boolean isBooleanVec;
	protected boolean isCountIncluded;
	protected  int intersectionLength;
	protected String matcherClass;
	protected String matchContext;
	
	
	/**
	 * @param src
	 * @param target
	 * @return
	 */
	public  double findDistance(String src, String target) {
		return 1.0;
	}
	
	public  double findDistance(String srcEntityID, String src, String targetEntityID, String target, String groupingID) {
		return 1.0;
	}
	
	/**
	 * @return
	 */
	public String getFieldDelimRegex() {
		return fieldDelimRegex;
	}

	/**
	 * @param fieldDelimRegex
	 */
	public void setFieldDelimRegex(String fieldDelimRegex) {
		this.fieldDelimRegex = fieldDelimRegex;
	}

	/**
	 * @return
	 */
	public boolean isBooleanVec() {
		return isBooleanVec;
	}

	/**
	 * @param isBooleanVec
	 */
	public void setBooleanVec(boolean isBooleanVec) {
		this.isBooleanVec = isBooleanVec;
	}
	
	/**
	 * @return
	 */
	public boolean isCountIncluded() {
		return isCountIncluded;
	}

	/**
	 * @param isCountIncluded
	 */
	public void setCountIncluded(boolean isCountIncluded) {
		this.isCountIncluded = isCountIncluded;
	}
	
	/**
	 * @return
	 */
	public int getIntersectionLength() {
		return intersectionLength;
	}

	public String getMatcherClass() {
		return matcherClass;
	}

	public void setMatcherClass(String matcherClass) {
		this.matcherClass = matcherClass;
	}

	public String getMatchContext() {
		return matchContext;
	}

	public void setMatchContext(String matchContext) {
		this.matchContext = matchContext;
	}

	/**
	 * @param algorithm
	 * @param params
	 * @return
	 */
	public static DynamicAttrSimilarityStrategy createSimilarityStrategy(String algorithm, Map<String,Object> params) {
		DynamicAttrSimilarityStrategy  simStrategy = null;
		if (algorithm.equals("jaccard")){
			double srcNonMatchingTermWeight = (Double)params.get("srcNonMatchingTermWeight");
			double trgNonMatchingTermWeight = (Double)params.get("trgNonMatchingTermWeight");
			simStrategy = new JaccardSimilarity(srcNonMatchingTermWeight, trgNonMatchingTermWeight);
		} else if (algorithm.equals("cosine")){
			simStrategy = new CosineSimilarity();
		}
		return simStrategy;
	}	
	
}
