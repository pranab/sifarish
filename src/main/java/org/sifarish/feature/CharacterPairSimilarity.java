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

public class CharacterPairSimilarity extends DynamicAttrSimilarityStrategy {

	public double findDistance(String src, String target) throws IOException {
		double dist = 0;
		Set<String> intersection = new HashSet<String>();
		
		String[] srcTerms = src.split(fieldDelimRegex);
		List<String> srcPairs = getCharacterPairs(srcTerms, intersection);
		String[] trgTerms = target.split(fieldDelimRegex);
		List<String> trgPairs = getCharacterPairs(trgTerms, intersection);
		
		double sim = (2.0 * intersection.size()) / (srcPairs.size() + trgPairs.size());
		dist = 1.0 -sim;
		return dist;
	}
	
	/**
	 * return all 2 char pairs
	 * @param terms
	 * @return
	 */
	private List<String> getCharacterPairs(String[] terms, Set<String> intersection) {
		List<String> pairs = new ArrayList<String>();
		for (String term : terms) {
			for (int i = 0; i < term.length() - 1; ++i) {
				String pair = term.substring(i, i+2); 
				pairs.add(pair);
				intersection.add(pair);
			}
		}
		return pairs;
	}
}
