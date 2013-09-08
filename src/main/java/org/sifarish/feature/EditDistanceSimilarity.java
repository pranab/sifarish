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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

/**
 * Distance based on edit distance of corresponding tokens in text
 * @author pranab
 *
 */
public class EditDistanceSimilarity extends DynamicAttrSimilarityStrategy {
	private Set<String> sequences = new HashSet<String>();
	private int maxSeqLength = 0;
	private static final int MIN_TOKEN_LENGTH = 1;
	private boolean tokenWise;
	

	public EditDistanceSimilarity(boolean tokenWise) {
		super();
		this.tokenWise = tokenWise;
	}

	
	/* (non-Javadoc)
	 * @see org.sifarish.feature.DynamicAttrSimilarityStrategy#findDistance(java.lang.String, java.lang.String)
	 */
	@Override
	public double findDistance(String src, String target) {
		double distance = 0;
		if (tokenWise) {
			distance = findDistanceTokenWise( src,  target);
		} else {
			distance = findDistanceFieldWise( src,  target);
		}
		
		return distance;
	}
	
	/**
	 * @param src
	 * @param target
	 * @return
	 */
	private  double findDistanceTokenWise(String src, String target) {
		double distance = 0;
		int editDistance = 0;
		//System.out.println("findDistanceTokenWise:" + src + ":"  + target);

		String[] srcTerms = src.split(fieldDelimRegex);
		String[] trgTerms = target.split(fieldDelimRegex);
		if (srcTerms.length == trgTerms.length) {
			for (int i = 0;  i  < srcTerms.length;  ++i ) {
				String srcItem =  srcTerms[i];
				String trgItem  = trgTerms[i];
				editDistance = 0;
			
				//only if tokens are not equal
				if (!srcItem.equals(trgItem)) {
					if (srcItem.length() == 1) {
						if (trgItem.indexOf(srcItem) >= 0) {
							editDistance  = trgItem.length() - 1;
						} else {
							editDistance  = trgItem.length() + 1;
						}
					} else if (trgItem.length() == 1) {
						if (srcItem.indexOf(srcItem) >= 0) {
							editDistance  = srcItem.length() - 1;
						} else {
							editDistance  =srcItem.length() + 1;
						}
					} else {
						sequences.clear();
						maxSeqLength = 0;
						generateSubSequences(srcItem, true);
						generateSubSequences(trgItem, false);
						editDistance  = srcItem.length() + trgItem.length() - 2 * maxSeqLength;
					}
				}
				
				//normalize
				distance += ((double)editDistance) / (srcItem.length() + trgItem.length() );
			}
			
			//average over number of tokens
			distance  /= srcTerms.length;
		} else {
			//unequal number of tokens
			distance = 1.0;
		}
		
		//System.out.println("edit distance:" + distance);
		return distance;
	}
	
	/**
	 * @param src
	 * @param target
	 * @return
	 */
	private  double findDistanceFieldWise(String src, String target) {
		double distance = 0;
		int editDistance = 0;
		sequences.clear();
		maxSeqLength = 0;
		generateSubSequences(src, true);
		generateSubSequences(target, false);
		editDistance  = src.length() + target.length() - 2 * maxSeqLength;
		distance += ((double)editDistance) / (src.length() + target.length() );

		return distance;
	}
	
	/**
	 * @param token
	 * @param store
	 */
	private void generateSubSequences(String token, boolean store) {
		int len = token.length();
		if (store) {
			sequences.add(token);
		} else {
			if (sequences.contains(token) && len > maxSeqLength) {
				maxSeqLength = len;
			}
		}
		
		String subToken = null;
		if (len  > MIN_TOKEN_LENGTH ) { 
			//create sub sequences by taking one char out and make recursive call
			for (int i = 0; i < len; ++i) {
				if (i == 0) {
					subToken = token.substring(1);
				} else if (i ==  len - 1) {
					subToken = token.substring(0, len - 1);
				} else {
					subToken = token.substring(0, i ) + token.substring(i + 1);
				}
				generateSubSequences(subToken, store);
			}
		}
	}
}
