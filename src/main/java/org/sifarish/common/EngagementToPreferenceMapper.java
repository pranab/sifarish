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

package org.sifarish.common;

import java.util.List;

/**
 * Converts engagement event to preference score
 * @author pranab
 *
 */
public class EngagementToPreferenceMapper {
	private List<EngagementScore> eventScores;

	public EngagementToPreferenceMapper() {
	}

	public List<EngagementScore> getEventScores() {
		return eventScores;
	}

	public void setEventScores(List<EngagementScore> eventScores) {
		this.eventScores = eventScores;
	}

	/**
	 * @param eventType
	 * @param count
	 * @return
	 */
	public int scoreForEvent(int eventType, int count) {
		int score;
		
		//match event type
		EngagementScore thisScore = null;
		for (EngagementScore engScore : eventScores) {
			if (engScore.getEventType() == eventType) {
				thisScore = engScore;
				break;
			}
		}
		
		//get score
		if (null != thisScore) {
			score = thisScore.eventScore(count);
		} else {
			throw new IllegalArgumentException("invlid engaement event type");
		}
		return score;
	}
}
