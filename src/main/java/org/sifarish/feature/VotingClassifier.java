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
import java.util.List;
import java.util.Map;

/**
 * Majority voting knn classifier
 * @author pranab
 *
 */
public class VotingClassifier extends NearestNeighborClassifier {

	/* (non-Javadoc)
	 * @see org.sifarish.feature.NearestNeighborClassifier#classify(java.util.List)
	 */
	@Override
	public String classify(List<String> neighbors) {
		String clazz = null;
		Map<String, Integer> voteCounter = new HashMap<String, Integer>();
		
		for (String neighbor :  neighbors) {
			String thisClazz = neighbor.split(",")[2];
			Integer count = voteCounter.get(thisClazz);
			if (null == count) {
				count = 1;
			} else {
				count = count + 1;
			}
			voteCounter.put(thisClazz, count);
		}
		
		// find max vote
		int max = 0;
		for ( Map.Entry<String, Integer> entry : voteCounter.entrySet() ) {
			if (entry.getValue() > max) {
				max = entry.getValue();
				clazz = entry.getKey();
			}
		}
		
		return clazz;
	}

}
