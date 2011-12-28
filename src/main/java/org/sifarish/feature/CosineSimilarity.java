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

public class CosineSimilarity  extends TextSimilarityStrategy{
	private Map<String, int[]> countVec = new HashMap<String, int[]>();

	@Override
	public double findDistance(String src, String target) {
		double distance = 1.0;
		countVec.clear();
		
		String[] srcTerms = src.split("\\s+");
		String[] trgTerms = target.split("\\s+");
		
		//count vectors
		for (String srcTerm  :  srcTerms){
			 int[] vec = countVec.get(srcTerm);
			 if (null == vec) {
				 vec = new int[2];
				 vec[0] = vec[1] = 0;
				 countVec.put(srcTerm, vec);
			 }
			 vec[0] += 1;
		}
		
		for (String trgTerm  :  trgTerms){
			 int[] vec = countVec.get(trgTerm);
			 if (null == vec) {
				 vec = new int[2];
				 vec[0] = vec[1] = 0;
				 countVec.put(trgTerm, vec);
			 }
			 vec[1] += 1;
		}
		
		//distance
		int crossProd = 0;
		int srcSqSum = 0;
		int trgSqSum = 0;
		
		for (Entry<String, int[]>  entry :  countVec.entrySet()) {
			int[] val = entry.getValue();
			crossProd += val[0] * val[1];
			srcSqSum += val[0] * val[0];
			trgSqSum += val[1] * val[1];
		}
		distance = ((double)crossProd) /( Math.sqrt(srcSqSum) * Math.sqrt(trgSqSum));
		return distance;
	}

}
