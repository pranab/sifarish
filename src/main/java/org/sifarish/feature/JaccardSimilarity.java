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

public class JaccardSimilarity extends TextSimilarityStrategy {
	private double nonMatchingTermWeight;

	
	public JaccardSimilarity(double nonMatchingTermWeight) {
		super();
		this.nonMatchingTermWeight = nonMatchingTermWeight;
	}


	@Override
	public double findDistance(String src, String target) {
		double distance = 1.0;
		
		String[] srcTerms = src.split("\\s+");
		String[] trgTerms = target.split("\\s+");
		
		int matchCount = 0;
		for (String srcTerm : srcTerms) {
			for (String trgTerm : trgTerms) {
				if (srcTerm.equals(trgTerm)) {
					++matchCount;
				}
			}
		}
		
		int nonMatchCount = srcTerms.length - matchCount + trgTerms.length - matchCount;
		distance = 1.0 - (double)matchCount / ((double)matchCount + nonMatchingTermWeight * nonMatchCount);
		return distance;
	}

}
