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

import org.sifarish.util.Field;
import org.sifarish.util.IDistanceStrategy;

/**
 * Distance calculation strategy
 * @author pranab
 *
 */
public abstract class DistanceStrategy  implements  IDistanceStrategy {
	protected double sumWt;
	protected int scale;
	protected double power;
	protected double totalWt;
	protected int count;
	
	protected enum DistanceStatus {
	    DistanceImploded, 
	    DistanceExploded, 
	    DistanceUntouched 
	}
	
	private Map<Integer, DistanceStatus> attributeDistanceStatus = new HashMap<Integer, DistanceStatus>();
	
	/**
	 * @param scale
	 */
	public DistanceStrategy(int scale) {
		this.scale = scale;
	}

	/**
	 * 
	 */
	public void initialize() {
		sumWt = 0.0;
		totalWt = 0.0;
		count = 0;
		attributeDistanceStatus.clear();
	}

	/**
	 * @param distance
	 * @param weight
	 */
	public abstract void accumulate(double distance, double weight);
	
	/**
	 * @param distance
	 * @param field
	 */
	public abstract void accumulate(double distance, Field field);
	
	/**
	 * @param distance
	 * @param field
	 */
	protected double getEffectiveDistance(double distance, Field field){
		double effectDist = 0;
		String distanceFunction = field.getContAttrDistanceFunction();
		double weight = field.getWeight(); 
		double threshold = field.getFunctionThreshold();

		if (distanceFunction.equals("none")) {
			effectDist = distance;
		} else if (distanceFunction.equals("nonLinear")) {
			//if weight < 1 then convex i.e. effective distance greater than distance otherwise concave
			effectDist =  (1 / weight) * distance  + ( 1 - 1 / weight) * distance * distance;
		} else if (distanceFunction.equals("sigmoid")) {
			//transtion at threshold, higher weight will simulate step function
			effectDist = 1.0 / (1 + Math.exp(-weight * (distance - threshold)));
		} else if (distanceFunction.equals("step")) {
			//transition at threshold
			effectDist = distance < threshold ? 0 : 1;
		} else if (distanceFunction.equals("ramp")) {
			//transtion at threshold
			effectDist = distance < threshold ? 0 : distance;
		} 
		
		//check for distance implosion and explosion
		if (effectDist < field.getImplodeThreshold()) {
			attributeDistanceStatus.put(field.getOrdinal(), DistanceStatus.DistanceImploded);
		} else if (effectDist > field.getExplodeThreshold()) {
			attributeDistanceStatus.put(field.getOrdinal(), DistanceStatus.DistanceExploded);
		}
		
		return effectDist;
	}
	
	/**
	 * @return
	 */
	protected DistanceStatus getDistanceStatus() {
		DistanceStatus status = DistanceStatus.DistanceUntouched;
		int explodedCount = 0;
		int implodedCount = 0;
		
		for (Map.Entry<Integer, DistanceStatus> entry : attributeDistanceStatus.entrySet()) {
			if (entry.getValue() == DistanceStatus.DistanceExploded) {
				++explodedCount;
			} else {
				++implodedCount;
			}
		}
		
		//explode or implode only when there is consensus
		if (explodedCount > 0 && implodedCount == 0) {
			status = DistanceStatus.DistanceExploded;
		} else if (implodedCount > 0 && explodedCount == 0) {
			status = DistanceStatus.DistanceImploded;
		}
		return status;
	}
	
	/**
	 * @return
	 */
	public abstract int getSimilarity();

	/**
	 * @param isScaled
	 * @return
	 */
	public abstract  double getSimilarity(boolean isScaled);
	
	/**
	 * @return
	 */
	public int getScale() {
		return scale;
	}

	/**
	 * @param scale
	 */
	public void setScale(int scale) {
		this.scale = scale;
	}

	/**
	 * @return
	 */
	public double getPower() {
		return power;
	}

	/**
	 * @param power
	 */
	public void setPower(double power) {
		this.power = power;
	}

}
