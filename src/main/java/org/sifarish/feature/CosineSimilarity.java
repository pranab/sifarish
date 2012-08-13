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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Cosine distance
 * @author pranab
 *
 */
public class CosineSimilarity  extends DynamicAttrSimilarityStrategy{
	private Map<String, int[]> countVec = new HashMap<String, int[]>();
	private int intersectionLength;

	/* (non-Javadoc)
	 * @see org.sifarish.feature.DynamicAttrSimilarityStrategy#findDistance(java.lang.String, java.lang.String)
	 */
	@Override
	public double findDistance(String src, String target) {
		double distance = 1.0;
		countVec.clear();
		intersectionLength = 0;
		
		String[] srcTerms = src.split(fieldDelimRegex);
		String[] trgTerms = target.split(fieldDelimRegex);
		
		//count vectors
		initVector(srcTerms, 0);
		initVector(trgTerms, 1);
		
		//distance
		int crossProd = 0;
		int srcSqSum = 0;
		int trgSqSum = 0;
		
		for (Entry<String, int[]>  entry :  countVec.entrySet()) {
			int[] val = entry.getValue();
			crossProd += val[0] * val[1];
			srcSqSum += val[0] * val[0];
			trgSqSum += val[1] * val[1];
			if (val[0] > 0 && val[1] > 0) {
				++intersectionLength;
			}
		}
		distance = ((double)crossProd) /( Math.sqrt(srcSqSum) * Math.sqrt(trgSqSum));
		if (distance > 0) {
			System.out.println("source:" + src);
			System.out.println("target:" + target);
			System.out.println("distance: " + distance);
		}
		return distance;
	}
	
	/**
	 * @param terms
	 * @param which
	 */
	private void initVector(String[] terms, int which) {
		String attr;
		int count = 0;
		for (String term  : terms){
			term = term.trim();
			if (isCountIncluded){
				String[] items = term.split(":");
				term = items[0];
				count = Integer.parseInt(items[1]);
			} 
			
			 int[] vec = countVec.get(term);
			 if (null == vec) {
				 vec = new int[2];
				 vec[0] = vec[1] = 0;
				 countVec.put(term, vec);
			 }
			 vec[which] = isBooleanVec ?  1 : ( isCountIncluded? count : vec[which] + 1);
		}
		
	}


}
