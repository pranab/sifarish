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

/**
 * @author pranab
 *
 */
public class DiceSimilarity extends DynamicAttrSimilarityStrategy {

	private DynamicAttrSimilarityStrategy jaccSimStrategy = new JaccardSimilarity(1.0, 1.0);
	
	/* (non-Javadoc)
	 * @see org.sifarish.feature.DynamicAttrSimilarityStrategy#findDistance(java.lang.String, java.lang.String)
	 */
	public double findDistance(String src, String target) throws IOException {
		//similarity is twice the jaccard
		double dist = jaccSimStrategy.findDistance(src, target);
		dist = 1.0 - 2 * (1 - dist);
		dist = dist < 0 ? 0 : dist;
		return dist;
	}
	
}
