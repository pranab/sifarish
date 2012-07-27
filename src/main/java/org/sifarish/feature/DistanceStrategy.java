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

/**
 * Distance calculation strategy
 * @author pranab
 *
 */
public abstract class DistanceStrategy {
	protected double sumWt;
	protected int scale;
	protected double power;
	protected double totalWt;
	protected int count;
	
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
	}

	/**
	 * @param distance
	 * @param weight
	 */
	public abstract void accumulate(double distance, double weight);
	
	/**
	 * @return
	 */
	public abstract int getSimilarity();

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
