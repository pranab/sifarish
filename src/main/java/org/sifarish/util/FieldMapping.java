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

package org.sifarish.util;

import java.util.ArrayList;
import java.util.List;

public class FieldMapping {
	private int matchingOrdinal = -1;
	private List<FieldMapping.ValueMapping> valueMappings;
	
	public int getMatchingOrdinal() {
		return matchingOrdinal;
	}

	public void setMatchingOrdinal(int matchingOrdinal) {
		this.matchingOrdinal = matchingOrdinal;
	}

	public List<FieldMapping.ValueMapping> getValueMappings() {
		return valueMappings;
	}

	public void setValueMappings(List<FieldMapping.ValueMapping> valueMappings) {
		this.valueMappings = valueMappings;
	}

	public static class ValueMapping {
		private String thisValue;
		private int[] thisValueRange;
		private String thatValue;
		
		public String getThisValue() {
			return thisValue;
		}
		public void setThisValue(String thisValue) {
			this.thisValue = thisValue;
		}
		public int[] getThisValueRange() {
			return thisValueRange;
		}
		public void setThisValueRange(int[] thisValueRange) {
			this.thisValueRange = thisValueRange;
		}
		public String getThatValue() {
			return thatValue;
		}
		public void setThatValue(String thatValue) {
			this.thatValue = thatValue;
		}
	}

}
