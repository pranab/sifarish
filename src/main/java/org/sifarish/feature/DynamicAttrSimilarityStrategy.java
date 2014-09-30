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
import java.util.Map;

/**
 * Entity attributes are variable and dynamic
 * @author pranab
 *
 */
public abstract class DynamicAttrSimilarityStrategy {
	protected String fieldDelimRegex = "\\s+";
	protected boolean isBooleanVec;
	protected boolean isSemanticVec;
	protected boolean isCountIncluded;
	protected  int intersectionLength;
	protected String[] matchingContexts;
	
	
	/**
	 * @param src
	 * @param target
	 * @return
	 */
	public  double findDistance(String src, String target)  throws IOException {
		return 1.0;
	}
	
	public  double findDistance(String srcEntityID, String src, String targetEntityID, String target, String groupingID)  throws IOException {
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
	public boolean isSemanticVec() {
		return isSemanticVec;
	}

	/**
	 * @param isSemanticVec
	 */
	public void setSemanticVec(boolean isSemanticVec) {
		this.isSemanticVec = isSemanticVec;
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

	public String[] getMatchingContexts() {
		return matchingContexts;
	}

	/**
	 * @param algorithm
	 * @param params
	 * @return
	 * @throws IOException 
	 */
	public static DynamicAttrSimilarityStrategy createSimilarityStrategy(String algorithm, Map<String,Object> params) 
		throws IOException {
		DynamicAttrSimilarityStrategy  simStrategy = null;
		if (algorithm.equals("jaccard")){
			double srcNonMatchingTermWeight = (Double)params.get("srcNonMatchingTermWeight");
			double trgNonMatchingTermWeight = (Double)params.get("trgNonMatchingTermWeight");
			simStrategy = new JaccardSimilarity(srcNonMatchingTermWeight, trgNonMatchingTermWeight);
		}else if (algorithm.equals("dice")){
			simStrategy = new DiceSimilarity();
		} else if (algorithm.equals("cosine")){
			simStrategy = new CosineSimilarity();
		} else if (algorithm.equals("semantic")){
			String matcherClass =(String) params.get("matcherClass");
			int  topMatchCount =(Integer) params.get("topMatchCount");
			simStrategy = new SemanticSimilarity(matcherClass, topMatchCount, params);
		} 
		return simStrategy;
	}	
	
}
