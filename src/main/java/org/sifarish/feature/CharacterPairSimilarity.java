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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Finds distance with matching character pair algorithms
 * @author pranab
 *
 */
public class CharacterPairSimilarity extends DynamicAttrSimilarityStrategy {

	/* (non-Javadoc)
	 * @see org.sifarish.feature.DynamicAttrSimilarityStrategy#findDistance(java.lang.String, java.lang.String)
	 */
	public double findDistance(String src, String target) throws IOException {
		double dist = 0;
		String[] srcTerms = src.split(fieldDelimRegex);
		List<String> srcPairs = getCharacterPairs(srcTerms);
		String[] trgTerms = target.split(fieldDelimRegex);
		List<String> trgPairs = getCharacterPairs(trgTerms);
		
		int union = srcPairs.size() + trgPairs.size();
		int intersection = findIntersection(srcPairs, trgPairs);
		double sim = (2.0 * intersection) / union;
		dist = 1.0 -sim;
		return dist;
	}
	
	/**
	 * return all 2 char pairs
	 * @param terms
	 * @return
	 */
	private List<String> getCharacterPairs(String[] terms) {
		List<String> pairs = new ArrayList<String>();
		for (String term : terms) {
			for (int i = 0; i < term.length() - 1; ++i) {
				String pair = term.substring(i, i+2); 
				pairs.add(pair);
			}
		}
		return pairs;
	}
	
	/**
	 * @param srcPairs
	 * @param trgPairs
	 * @return
	 */
	private int findIntersection(List<String> srcPairs, List<String> trgPairs) {
		int intersection = 0;
		Set<Integer> matchedTrg = new HashSet<Integer>();
		for (int i = 0; i < srcPairs.size(); ++i) {
			String srcPair = srcPairs.get(i);
			for (int j = 0; i < trgPairs.size(); ++j) {
				String trgPair = trgPairs.get(j);
				if (srcPair.equals(trgPair) && !matchedTrg.contains(j)) {
					++intersection;
					matchedTrg.add(j);
					break;
				}
				
			}
		}
		return intersection;
	}
}
