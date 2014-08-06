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

import java.text.ParseException;

/**
 * @author pranab
 *
 */
public class HourWindow extends StructuredAttribute  {
	private int start;
	private int end;
	
	public HourWindow(String startHourMin, String endHourMin) throws ParseException {
		int hourStart = Integer.parseInt(startHourMin.substring(0, 2));
		int minStart = Integer.parseInt(startHourMin.substring(2));
		start = hourStart * 60 + minStart;
		
		int hourEnd = Integer.parseInt(endHourMin.substring(0, 2));
		int minEnd = Integer.parseInt(endHourMin.substring(2));
		end = hourEnd * 60 + minEnd;
		
		if (this.start > this.end) {
			throw new IllegalArgumentException("invalid HourWindow start time should be less than end time start:" + start + 
					" end:" + end);
		}
	}
	
	@Override
	public double distance(StructuredAttribute otherAttr, Field field) {
		HourWindow other = (HourWindow)otherAttr;
		double distance = 0;
		long overlap = 0;
		//long min = getLength() < other.getLength() ?  getLength() : other.getLength();
		if (start < other.start) {
			if (end < other.start) {
				distance = 1;
			} else if (end < other.end) {
				overlap =  end - other.start;
			} else {
				overlap =  other.end - other.start;
			}
		}  else if (start < other.end) {
			if (end <= other.end) {
				overlap = end - start;
			} else {
				overlap = other.end - start;
			}
		} else {
			distance = 1;
		}
		
		if (overlap > 0) {
			distance =( (double)overlap) / field.getMaxTimeWindow();
		}
		return distance;
	}

}
