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

import java.util.List;

public class Field {
	private String name;
	private int ordinal = -1;
	private boolean type;
	private boolean id;
	private String dataType;
	private int min;
	private int max;
	private double weight = 1.0;
	private int matchingOrdinal = -1;
	private List<FieldMapping> mappings;
	private List<CategoricalDistance> categoricalDistances;
	private String numDistFunction = "equalSoft";
	private ConceptHierarchy conceptHierarchy;
	
	
	public boolean isType() {
		return type;
	}
	public void setType(boolean type) {
		this.type = type;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getOrdinal() {
		return ordinal;
	}
	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}
	public boolean isId() {
		return id;
	}
	public void setId(boolean id) {
		this.id = id;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public int getMatchingOrdinal() {
		return matchingOrdinal;
	}
	public void setMatchingOrdinal(int matchingOrdinal) {
		this.matchingOrdinal = matchingOrdinal;
	}
	public int getMin() {
		return min;
	}
	public void setMin(int min) {
		this.min = min;
	}
	public int getMax() {
		return max;
	}
	public void setMax(int max) {
		this.max = max;
	}
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	public List<FieldMapping> getMappings() {
		return mappings;
	}
	public void setMappings(List<FieldMapping> mappings) {
		this.mappings = mappings;
	}
	public List<CategoricalDistance> getCategoricalDistances() {
		return categoricalDistances;
	}
	public void setCategoricalDistances(List<CategoricalDistance> categoricalDistances) {
		this.categoricalDistances = categoricalDistances;
	}
	
	public String getNumDistFunction() {
		return numDistFunction;
	}
	public void setNumDistFunction(String numDistFunction) {
		this.numDistFunction = numDistFunction;
	}
	public ConceptHierarchy getConceptHierarchy() {
		return conceptHierarchy;
	}
	public void setConceptHierarchy(ConceptHierarchy conceptHierarchy) {
		this.conceptHierarchy = conceptHierarchy;
	}
	public double  findDistance(String thisValue, String thatValue) {
		double distance = 1.0;
		if (thisValue.equals(thatValue)) {
			//match
			distance = 0.0;
		} else {
			boolean overridden = false;
			if (null != categoricalDistances) {
				//try overridden categorical distance
				for (CategoricalDistance catDist : categoricalDistances) {
					if ( thisValue.equals(catDist.getThisValue()) && thatValue.equals(catDist.getThatValue())  || 
							thisValue.equals(catDist.getThatValue()) && thatValue.equals(catDist.getThisValue()) ) {
						distance = catDist.getDistance();
						overridden = true;
						break;
					}
				}
			}
			
			if (!overridden && null != conceptHierarchy) {
				//try concept hierarchy
				String parentThatValue = conceptHierarchy.findParent(thatValue);
				if (null != parentThatValue && thisValue.equals(parentThatValue)){
					distance = 0.0;
				}
			}
		}
		return distance;
	}
	
	
}
