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

import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class Location extends StructuredAttribute {
	private String landMark;
	private String city;
	private String state;
	
	/**
	 * @param landMark
	 * @param city
	 * @param state
	 */
	public Location(String landMark, String city, String state) {
		if (null != landMark) {
			this.landMark = Utility.normalize(landMark);
		}
		this.city = Utility.normalize(city);
		this.state = Utility.normalize(state);
	}
	
	/* (non-Javadoc)
	 * @see org.sifarish.util.StructuredAttribute#distance(org.sifarish.util.StructuredAttribute)
	 */
	public double distance(StructuredAttribute otherAttr, Field field) {
		IDistanceStrategy distStrategy = field.getDistStrategy();
		double[] weights = field.getComponentWeights();
		Location other = (Location)otherAttr;
		distStrategy.initialize();
		double dist;
		if (null  != landMark) {
			dist = (landMark.equals(other.landMark)? 0 : 1);
			distStrategy.accumulate(dist, weights[0]);
		}
		dist = city.equals(other.city)? 0 : 1;
		distStrategy.accumulate(dist, weights[1]);
		dist = state.equals(other.state)? 0 : 1;
		distStrategy.accumulate(dist, weights[2]);

		return distStrategy.getSimilarity(false);
	}

}
