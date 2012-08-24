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

import org.sifarish.common.TaggedEntity;

/**
 * @author pranab
 *
 */
public class SemanticSimilarity extends DynamicAttrSimilarityStrategy {
	private TaggedEntity  thisEntity;
	private TaggedEntity  thatEntity;
	
	public SemanticSimilarity(String matcherClass) throws IOException   {
        Class<?> iterCls;
		try {
			iterCls = Class.forName(matcherClass);
			thisEntity = (TaggedEntity)iterCls.newInstance();
			thatEntity = (TaggedEntity)iterCls.newInstance();
		} catch (ClassNotFoundException e) {
			throw new IOException("failed to intialize SemanticSimilarity");
		}catch (InstantiationException e) {
			throw new IOException("failed to intialize SemanticSimilarity");
		} catch (IllegalAccessException e) {
			throw new IOException("failed to intialize SemanticSimilarity");
		}
	}
	
	@Override
	public double findDistance(String thisEntityID, String thisTag,
			String thatEntityID, String thatTag, String groupingID) {
		thisEntity.setEntityID(thisEntityID);
		thisEntity.setGroupID(groupingID);
		thatEntity.setEntityID(thisEntityID);
		thatEntity.setGroupID(groupingID);
		
		int matchScoreMax = 0;
		int matchScore;
		String[] thisTagItems = thisTag.split(fieldDelimRegex);
		String[] thatTagItems = thatTag.split(fieldDelimRegex);
		for (String thisTagItem : thisTagItems) {
			thisEntity.setTag(thisTagItem);
			for (String thatTagItem :thatTagItems) {
				thatEntity.setTag(thatTagItem);
				matchScore = thisEntity.match(thatEntity);
				if (matchScore > matchScoreMax) {
					matchScoreMax = matchScore;
					matchingContext = thisEntity.matchingContext();
				}
			}
		}
		
		return ((double)matchScoreMax) / 10;
	}

}
