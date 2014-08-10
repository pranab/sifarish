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

import org.sifarish.feature.DistanceStrategy.DistanceStatus;
import org.sifarish.util.Field;

/**
 * Monowski distance
 * @author pranab
 *
 */
public class MinkwoskiDistance extends DistanceStrategy {

	/**
	 * @param scale
	 */
	public MinkwoskiDistance(int scale) {
		super(scale);
	}

	/* (non-Javadoc)
	 * @see org.sifarish.feature.DistanceStrategy#accumulate(double, double)
	 */
	@Override
	public void accumulate(double distance, double weight) {
		distance = Math.abs(distance);
		double effectDist =  (1 / weight) * distance  + ( 1 - 1 / weight) * distance * distance;
		sumWt += Math.pow(effectDist, power);
		++count;
	}

	/**
	 * @param distance
	 * @param field
	 */
	public void accumulate(double distance, Field field){
		distance = Math.abs(distance);
		double effectDist = getEffectiveDistance(distance, field);
		sumWt += Math.pow(effectDist, power);
		++count;
	}

	
	
	/* (non-Javadoc)
	 * @see org.sifarish.feature.DistanceStrategy#getSimilarity()
	 */
	@Override
	public int getSimilarity() {
		int sim = 0;
		DistanceStatus status = getDistanceStatus();
		if (status == DistanceStatus.DistanceUntouched) {
			sim = (int)((Math.pow(sumWt, 1.0/power)  * scale) / count);
		} else if (status == DistanceStatus.DistanceImploded) {
			sim = 0;
		} else if (status == DistanceStatus.DistanceExploded) {
			sim = scale;
		}
		
		return sim;
	}

	@Override
	public double getSimilarity(boolean isScaled) {
		double dist = 0;
		if (isScaled) {
			dist = ((Math.pow(sumWt, 1.0/power)  * scale) / count);
		} else {
			dist = ((Math.pow(sumWt, 1.0/power) ) / count);
		}
		return 0;
	}

}
