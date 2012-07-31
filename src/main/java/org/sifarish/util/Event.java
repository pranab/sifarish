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

/**
 * Event with location and time window
 * @author pranab
 *
 */
public class Event extends StructuredAttribute {
	private String description;
	private Location location;
	private TimeWindow timeWindow;
	private double[] locationWeights;

	public Event(String description, Location location, TimeWindow timeWindow, double[] locationWeights) {
		this.description = description;
		this.location = location;
		this.timeWindow = timeWindow;
		this.locationWeights = locationWeights;
	}

	@Override
	public double distance(StructuredAttribute otherAttr, Field field) {
		Event other = (Event)otherAttr;
		double dist = 0;
		//TODO
		
		return dist;
	}

}
