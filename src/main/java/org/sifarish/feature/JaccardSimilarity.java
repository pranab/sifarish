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

/**
 * Jaccard similarity
 * @author pranab
 *
 */
public class JaccardSimilarity extends DynamicAttrSimilarityStrategy {
	private double srcNonMatchingTermWeight;
	private double trgNonMatchingTermWeight;

	
	/**
	 * @param srcNonMatchingTermWeight
	 * @param trgNonMatchingTermWeight
	 */
	public JaccardSimilarity(double srcNonMatchingTermWeight, double trgNonMatchingTermWeight) {
		super();
		this.srcNonMatchingTermWeight = srcNonMatchingTermWeight;
		this.trgNonMatchingTermWeight = trgNonMatchingTermWeight;
	}


	/* (non-Javadoc)
	 * @see org.sifarish.feature.DynamicAttrSimilarityStrategy#findDistance(java.lang.String, java.lang.String)
	 */
	@Override
	public double findDistance(String src, String target) {
		double distance;
		
		String[] srcTerms = src.split(fieldDelimRegex);
		String[] trgTerms = target.split(fieldDelimRegex);
		
		int matchCount = 0;
		for (String srcTerm : srcTerms) {
			for (String trgTerm : trgTerms) {
				if (srcTerm.equals(trgTerm)) {
					++matchCount;
				}
			}
		}
		
		int srcNonMatchCount = srcTerms.length - matchCount;
		int trgNonMatchCount = trgTerms.length - matchCount;
		distance = 1.0 - (double)matchCount / ((double)matchCount + srcNonMatchingTermWeight * srcNonMatchCount +
				trgNonMatchingTermWeight * trgNonMatchCount);
		intersectionLength = matchCount;
		return distance;
	}

}
