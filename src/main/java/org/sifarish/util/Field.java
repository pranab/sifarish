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

/**
 * Field
 * @author pranab
 *
 */
public class Field {
	private String name;
	private int ordinal = -1;
	private boolean type;
	private boolean id;
	private boolean classAttribute;
	private String dataType;
	private String dataSubType = "none";
	private String textDataSubTypeFormat;
	private int min;
	private int max;
	private String unit = "";
	private double weight = 1.0;
	private int matchingOrdinal = -1;
	private List<FieldMapping> mappings;
	private List<CategoricalDistance> categoricalDistances;
	private String numDistFunction = "equalSoft";
	private ConceptHierarchy conceptHierarchy;
	private String distAlgorithm;
	private double[] componentWeights;
	private IDistanceStrategy distStrategy;
	private double distThreshold = -1.0;
	private int maxDistance;
	private int maxTimeWindow;
	private String contAttrDistanceFunction = "none";
	private double functionThreshold = 0.5;
	private double implodeThreshold = -0.1;
	private double explodeThreshold = 1.1;
	
	public static final String DATA_TYPE_STRING = "string";
	public static final String DATA_TYPE_CATEGORICAL = "categorical";
	public static final String DATA_TYPE_INT = "int";
	public static final String DATA_TYPE_DOUBLE = "double";
	public static final String DATA_TYPE_TEXT = "text";
	public static final String DATA_TYPE_TIME_WINDOW = "timeWindow";
	public static final String DATA_TYPE_HOUR_WINDOW = "hourWindow";
	public static final String DATA_TYPE_LOCATION = "location";
	public static final String DATA_TYPE_GEO_LOCATION = "geoLocation";
	public static final String DATA_TYPE_EVENT = "event";

	public static final String TEXT_TYPE_PERSON_NAME = "personName";
	public static final String TEXT_TYPE_STREET_ADDRESS = "streetAddress";
	public static final String TEXT_TYPE_STREET_ADDRESS_ONE = "streetAddressOne";
	public static final String TEXT_TYPE_STREET_ADDRESS_TWO = "streetAddressTwo";
	public static final String TEXT_TYPE_CITY = "city";
	public static final String TEXT_TYPE_STATE = "state";
	public static final String TEXT_TYPE_ZIP = "zip";
	public static final String TEXT_TYPE_COUNTRY = "country";
	public static final String TEXT_TYPE_PHONE_NUM = "phoneNum";
	public static final String TEXT_TYPE_EMAIL_ADDR = "emailAddress";
	
	
	
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
	public boolean isClassAttribute() {
		return classAttribute;
	}
	public void setClassAttribute(boolean classAttribute) {
		this.classAttribute = classAttribute;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getDataSubType() {
		return dataSubType;
	}
	public void setDataSubType(String dataSubType) {
		this.dataSubType = dataSubType;
	}
	public String getTextDataSubTypeFormat() {
		return textDataSubTypeFormat;
	}
	public void setTextDataSubTypeFormat(String textDataSubTypeFormat) {
		this.textDataSubTypeFormat = textDataSubTypeFormat;
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
	public String getUnit() {
		return unit;
	}
	public void setUnit(String unit) {
		this.unit = unit;
	}
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
		this.contAttrDistanceFunction = "nonLinear";
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
	
	public String getDistAlgorithm() {
		return distAlgorithm;
	}
	public void setDistAlgorithm(String distAlgorithm) {
		this.distAlgorithm = distAlgorithm;
	}
	public double[] getComponentWeights() {
		return componentWeights;
	}
	public void setComponentWeights(double[] componentWeights) {
		this.componentWeights = componentWeights;
	}
	/**
	 * Distance between categorical
	 * @param thisValue
	 * @param thatValue
	 * @return
	 */
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
	
	/**
	 * Distance between int values
	 * @param thisValue
	 * @param thatValue
	 * @param diffThreshold
	 * @return
	 */
	public double  findDistance(int thisValue, int thatValue,  double diffThreshold) {
		double distance = 1.0;
		if (max > min) {
			distance = ((double)(thisValue - thatValue)) / (max  - min);
		} else {
			int max = thisValue > thatValue ? thisValue : thatValue;
			double diff = ((double)(thisValue - thatValue)) / max;
			if (diff < 0) {
				diff = - diff;
			}
			distance = diff > diffThreshold ? 1.0 : 0.0;
				
		}
		if (distance < 0) {
			distance = -distance;
		}
		
		return distance;
	}	

	/**
	 * Distance between double values
	 * @param thisValue
	 * @param thatValue
	 * @param diffThreshold
	 * @return
	 */
	public double  findDistance(double thisValue, double thatValue,  double diffThreshold) {
		double distance = 1.0;
		if (max > min) {
			distance = ((thisValue - thatValue)) / (max  - min);
		} else {
			double  max = thisValue > thatValue ? thisValue : thatValue;
			double diff = (thisValue - thatValue)/ max;
			if (diff < 0) {
				diff = - diff;
			}
			distance = diff > diffThreshold ? 1.0 : 0.0;
				
		}
		if (distance < 0) {
			distance = -distance;
		}
		
		return distance;
	}	

	/**
	 * Distance between structured attributes
	 * @param thisValue
	 * @param thatValue
	 * @return
	 */
	public double  findDistance(StructuredAttribute  thisValue, StructuredAttribute  thatValue ) {
		return  thisValue.distance(thatValue, this);
	}
	
	public IDistanceStrategy getDistStrategy() {
		return distStrategy;
	}
	public void setDistStrategy(IDistanceStrategy distStrategy) {
		this.distStrategy = distStrategy;
	}
	public double getDistThreshold() {
		return distThreshold;
	}
	public void setDistThreshold(double distThreshold) {
		this.distThreshold = distThreshold;
	}
	
	public boolean isDistanceThresholdCrossed(double dist) {
		return distThreshold > 0.0 && dist > distThreshold;
	}
	public int getMaxDistance() {
		return maxDistance;
	}
	public void setMaxDistance(int maxDistance) {
		this.maxDistance = maxDistance;
	}
	public int getMaxTimeWindow() {
		return maxTimeWindow;
	}

	public long  getMaxTimeWindowInMili() {
		return maxTimeWindow * 60 * 1000L;
	}
	
	public void setMaxTimeWindow(int maxTimeWindow) {
		this.maxTimeWindow = maxTimeWindow;
	}
	public String getContAttrDistanceFunction() {
		return contAttrDistanceFunction;
	}
	public void setContAttrDistanceFunction(String contAttrDistanceFunction) {
		this.contAttrDistanceFunction = contAttrDistanceFunction;
	}
	public double getFunctionThreshold() {
		return functionThreshold;
	}
	public void setFunctionThreshold(double functionThreshold) {
		this.functionThreshold = functionThreshold;
	}
	public double getImplodeThreshold() {
		return implodeThreshold;
	}
	public void setImplodeThreshold(double implodeThreshold) {
		this.implodeThreshold = implodeThreshold;
	}
	public double getExplodeThreshold() {
		return explodeThreshold;
	}
	public void setExplodeThreshold(double explodeThreshold) {
		this.explodeThreshold = explodeThreshold;
	}
}
