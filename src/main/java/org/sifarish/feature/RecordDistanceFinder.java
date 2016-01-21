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

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.sifarish.etl.StructuredTextNormalizer;
import org.sifarish.util.Event;
import org.sifarish.util.Field;
import org.sifarish.util.HourWindow;
import org.sifarish.util.Location;
import org.sifarish.util.TimeWindow;

/**
 * Finds distance between two records. Supports various kinds of attributes
 * @author pranab
 *
 */
public class RecordDistanceFinder {
	private boolean mixedInSets;
	private String fieldDelimRegex;
	private int idOrdinal;
	private int setIdSize;
	private int distThreshold;
	private DistanceStrategy distStrategy;
	private SingleTypeSchema schema;
	private int[] facetedFields;
	private boolean includePassiveFields;
	private int[] passiveFields;
	private DynamicAttrSimilarityStrategy textSimStrategy;
	private String subFieldDelim;
	private StructuredTextNormalizer textNormalizer;
	
    /**
     * @param fieldDelimRegex
     * @param idOrdinal
     * @param distThreshold
     * @param distStrategy
     * @param schema
     * @param textSimStrategy
     * @param subFieldDelim
     */
    public RecordDistanceFinder(String fieldDelimRegex, int idOrdinal, int scale,
			int distThreshold, SingleTypeSchema schema, String subFieldDelim,
			StructuredTextNormalizer textNormalizer) {
		super();
		this.fieldDelimRegex = fieldDelimRegex;
		this.idOrdinal = idOrdinal;
		this.distThreshold = distThreshold;
		this.distStrategy = schema.createDistanceStrategy(scale);
		this.schema = schema;
		this.textSimStrategy =schema.createTextSimilarityStrategy();
		this.subFieldDelim = subFieldDelim;
		this.textNormalizer = textNormalizer;
	}

    /**
     * @param fieldDelimRegex
     * @param idOrdinal
     * @param scale
     * @param distThreshold
     * @param schema
     * @param subFieldDelim
     */
    public RecordDistanceFinder(String fieldDelimRegex, int idOrdinal, int scale,
			int distThreshold, SingleTypeSchema schema, String subFieldDelim) {
    	this(fieldDelimRegex, idOrdinal, scale, distThreshold, schema, subFieldDelim,null);
    }
    
    /**
     * @param mixedInSets
     * @return
     */
    public RecordDistanceFinder withMixedInSets(boolean mixedInSets) {
    	this.mixedInSets = mixedInSets;
    	return this;
    }
    
    /**
     * @param setIdSize
     * @return
     */
    public RecordDistanceFinder withSetIdSize(int setIdSize) {
    	this.setIdSize = setIdSize;
    	return this;
    }

    /**
     * @param facetedFields
     * @return
     */
    public RecordDistanceFinder withFacetedFields(int[] facetedFields) {
    	this.facetedFields = facetedFields;
    	return this;
    }
    
    /**
     * @param includePassiveFields
     * @return
     */
    public RecordDistanceFinder withIncludePassiveFields(boolean includePassiveFields) {
    	this.includePassiveFields = includePassiveFields;
    	return this;
    }

    /**
     * @param passiveFields
     * @return
     */
    public RecordDistanceFinder withPassiveFields(int[] passiveFields) {
    	this.passiveFields = passiveFields;
    	return this;
    }

    /**
     * @param first
     * @param second
     * @return
     * @throws IOException
     */
    public int findDistance(String first, String second) throws IOException {
		String[] firstItems = first.split(fieldDelimRegex);
		String[] secondItems = second.split(fieldDelimRegex);
		return findDistance(firstItems, secondItems);
    }
    
	/**
     * @param first
     * @param second
     * @return
     * @throws IOException 
     */
    public int findDistance(String[] firstItems, String[] secondItems) throws IOException {
		String firstId =  firstItems[idOrdinal];
		String secondId =  secondItems[idOrdinal];
		
    	int netDist = 0;

   		//if inter set matching with mixed in sets, match only same ID from different sets
    	if (mixedInSets) {
    		//entityID is concatenation of setID and real entityID
    		String firstEntityId = firstId.substring(setIdSize);
    		String secondEntityId = secondId.substring(setIdSize);
    		if (!firstEntityId.equals(secondEntityId)) {
    			netDist =  distThreshold + 1;
    			return netDist;
    		}
    	}
    	
		double dist = 0;
		boolean valid = false;
		distStrategy.initialize();
		List<Integer> activeFields = null;
		
		boolean thresholdCrossed = false;
		for (Field field :  schema.getEntity().getFields()) {
			if (null != facetedFields) {
				//if facetted set but field not included, then skip it
				if (!ArrayUtils.contains(facetedFields, field.getOrdinal())) {
					continue;
				}
			}
			
			//if ID or class attribute field, skip it
			if (field.isId() ||  field.isClassAttribute()) {
				continue;
			}
			
			//track fields participating is dist calculation
			if (includePassiveFields && null == passiveFields) {
				if (null == activeFields) {
					activeFields = new ArrayList<Integer>();
				}
				activeFields.add(field.getOrdinal());
			}
			
			//extract fields
			String firstAttr = "";
			if (field.getOrdinal() < firstItems.length ){
				firstAttr = firstItems[field.getOrdinal()];
			} else {
				throw new IOException("Invalid field ordinal. Looking for field " + field.getOrdinal() + 
						" found "  + firstItems.length + " fields in the record starting with :" + firstItems[0]);
			}
			
			String secondAttr = "";
			if (field.getOrdinal() < secondItems.length ){
				secondAttr = secondItems[field.getOrdinal()];
			}else {
				throw new IOException("Invalid field ordinal. Looking for field " + field.getOrdinal() + 
						" found "  + secondItems.length + " fields in the record starting with:" + secondItems[0]);
			} 
			String unit = field.getUnit();
			
			if (firstAttr.isEmpty() || secondAttr.isEmpty() ) {
				//handle missing value
				String missingValueHandler = schema.getMissingValueHandler();
				if (missingValueHandler.equals("default")) {
   				    dist = 1.0;
				} else if (missingValueHandler.equals("skip")) {
					continue;
				} else {
					//custom handler
					
				}
			} else {
				dist = 0;
    			if (field.getDataType().equals(Field.DATA_TYPE_CATEGORICAL)) {
    				//categorical
    				dist = field.findDistance(firstAttr, secondAttr);
    			} else if (field.getDataType().equals(Field.DATA_TYPE_INT)) {
    				//int
    				dist = numericDistance(field,  firstAttr,  secondAttr,  true);
    			} else if (field.getDataType().equals(Field.DATA_TYPE_DOUBLE)) {
    				//double
    				dist =  numericDistance( field,  firstAttr,  secondAttr, false);
    			} else if (field.getDataType().equals(Field.DATA_TYPE_TEXT)) { 
    				//text
    				dist = textDistance(field, firstAttr, secondAttr);
    			} else if (field.getDataType().equals(Field.DATA_TYPE_TIME_WINDOW)) {
    				//time window
    				dist = timeWindowDistance(field, firstAttr,  secondAttr);
    			} else if (field.getDataType().equals(Field.DATA_TYPE_HOUR_WINDOW)) {
    				//hour window
    				dist = hourWindowDistance(field, firstAttr,  secondAttr);
    			}   else if (field.getDataType().equals(Field.DATA_TYPE_LOCATION)) {
    				//location
    				dist = locationDistance(field, firstAttr,  secondAttr);
    			} else if (field.getDataType().equals(Field.DATA_TYPE_GEO_LOCATION)) {
    				//geo location
    				dist = geoLocationDistance(field, firstAttr,  secondAttr);
    			}  else if (field.getDataType().equals(Field.DATA_TYPE_EVENT)) {
    				//event
    				dist = eventDistance(field, firstAttr,  secondAttr);
    			}
			}
			
			//if threshold crossed for this attribute, skip the remaining attributes of the entity pair
			thresholdCrossed = field.isDistanceThresholdCrossed(dist);
			if (thresholdCrossed){
				break;
			}
			
			//aggregate attribute  distance for all entity attributes
			distStrategy.accumulate(dist, field);
		}  
		
		//initialize passive fields
		if (includePassiveFields && null == passiveFields) {
			intializePassiveFieldOrdinal(activeFields, firstItems.length);
		}
		
		netDist = thresholdCrossed?  distThreshold + 1  : distStrategy.getSimilarity();
		return netDist;
    }

    /**
     * @param field
     * @param firstAttr
     * @param secondAttr
     * @return
     * @throws IOException
     */
    private double textDistance(Field field, String firstAttr,  String secondAttr) throws IOException {
    	double dist = 0;
		if (field.getDataSubType() == Field.TEXT_TYPE_PERSON_NAME) {
			dist = personNameDistance(field, firstAttr, secondAttr);
		} if (field.getDataSubType() == Field.TEXT_TYPE_STREET_ADDRESS) {
			dist = streetAddressDistance(field, firstAttr, secondAttr);
		} else {
			dist = textSimStrategy.findDistance(firstAttr, secondAttr);	    
		}
		return dist;
    }
    
    /**
     * @param field
     * @param firstAttr
     * @param secondAttr
     * @return
     */
    private double numericDistance(Field field, String firstAttr,  String secondAttr, boolean isInt) {
    	double dist = 0;
		String[] firstValItems = firstAttr.split("\\s+");
		String[] secondValItems = secondAttr.split("\\s+");
		boolean valid = false;
		String unit = field.getUnit();

		if (firstValItems.length == 1 && secondValItems.length == 1){
			valid = true;
		} else if (firstValItems.length == 2 && secondValItems.length == 2 && 
				firstValItems[1].equals(unit) && secondValItems[1].equals(unit)) {
			valid = true;
		}
		
		if (valid) {
			try {
				if (isInt) {
    				dist = field.findDistance(Integer.parseInt(firstValItems[0]), Integer.parseInt(secondValItems[0]), 
        					schema.getNumericDiffThreshold());
				} else {
					dist = field.findDistance(Double.parseDouble(firstValItems[0]), Double.parseDouble(secondValItems[0]), 
						schema.getNumericDiffThreshold());
				}
			} catch (NumberFormatException nfEx) {
			}
		} else {
		}
    	return dist;
    }       
    
    /**
     * Distance as overlap between time ranges
     * @param field
     * @param firstAttr
     * @param secondAttr
     * @param context
     * @return
     */
    private double timeWindowDistance(Field field, String firstAttr, String secondAttr) {
    	double dist = 0;
		try {
			String[] subFields = firstAttr.split(subFieldDelim);
			TimeWindow firstTimeWindow = new TimeWindow(subFields[0], subFields[1]);
			subFields = secondAttr.split(subFieldDelim);
			TimeWindow secondTimeWindow = new TimeWindow(subFields[0], subFields[1]);

			dist = field.findDistance(firstTimeWindow, secondTimeWindow);
		} catch (ParseException e) {
			//context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
		}
    	return dist;
    }    

    /**
     * @param field
     * @param firstAttr
     * @param secondAttr
     * @param context
     * @return
     */
    private double hourWindowDistance(Field field, String firstAttr, String secondAttr) {
    	double dist = 0;
		try {
			String[] subFields = firstAttr.split(subFieldDelim);
			HourWindow firstTimeWindow = new HourWindow(subFields[0], subFields[1]);
			subFields = secondAttr.split(subFieldDelim);
			HourWindow secondTimeWindow = new HourWindow(subFields[0], subFields[1]);

			dist = field.findDistance(firstTimeWindow, secondTimeWindow);
		} catch (ParseException e) {
			//context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
		}
    	return dist;
    }    
    /**
     * @param field
     * @param firstAttr
     * @param secondAttr
     * @param context
     * @return
     */
    private double locationDistance(Field field, String firstAttr, String secondAttr) {
    	double dist = 0;
		String[] subFields = firstAttr.split(subFieldDelim);
		Location firstLocation  = new Location( subFields[0], subFields[1], subFields[2]); 
	    subFields = secondAttr.split(subFieldDelim);
		Location secondLocation  = new Location( subFields[0], subFields[1], subFields[2]); 

		dist = field.findDistance(firstLocation, secondLocation);
    	return dist;
    }    

    /**
     * @param field
     * @param firstAttr
     * @param secondAttr
     * @param context
     * @return
     */
    private double geoLocationDistance(Field field, String firstAttr, String secondAttr) {
		double dist = org.sifarish.util.Utility.getGeoDistance(firstAttr, secondAttr);
		dist /= field.getMaxDistance();
		dist = dist <= 1.0 ? dist : 1.0;
		return dist;
    }    

    /**
     * @param activeFields
     * @param numFields
     */
    private void intializePassiveFieldOrdinal(List<Integer> activeFields, int numFields) {
    	int len = numFields - activeFields.size();
    	if (len > 0) {
    		//all fields that are not active i.e not defined in schema
        	passiveFields = new int[len];
        	for (int i = 0,j=0; i < numFields; ++i) {
        		if (!activeFields.contains(i) ) {
        			passiveFields[j++] = i;
        		}
        	}
    	}
    }
    
    /**
     * @param field
     * @param firstAttr
     * @param secondAttr
     * @param context
     * @return
     */
    private double eventDistance(Field field, String firstAttr, String secondAttr) {
    	double dist = 0;
		try {
			double[] locationWeights = schema.getLocationComponentWeights();
			String[] subFields = firstAttr.split(subFieldDelim);
			String description = subFields[0];
			Location location  = new Location( subFields[1], subFields[2], subFields[3]); 
			TimeWindow timeWindow = new TimeWindow(subFields[4], subFields[5]);
			Event firstEvent = new Event(description, location, timeWindow, locationWeights);
			
			subFields = secondAttr.split(subFieldDelim);
			description = subFields[0];
			location  = new Location( subFields[1], subFields[2], subFields[3]); 
			timeWindow = new TimeWindow(subFields[4], subFields[5]);
			Event secondEvent = new Event(description, location, timeWindow, locationWeights);

			dist = field.findDistance(firstEvent, secondEvent);
		} catch (ParseException e) {
			//context.getCounter("Invalid Data Format", "Field:" + field.getOrdinal()).increment(1);
		}
    	return dist;
    }    
    
    /**
     * @param field
     * @param firstAttr
     * @param secondAttr
     * @return
     * @throws IOException
     */
    private double personNameDistance(Field field, String firstAttr, String secondAttr) throws IOException {
    	double dist = 0;
    	String[] firstItems = firstAttr.split("\\s+");
    	String[] secondItems = secondAttr.split("\\s+");
    	double firstNameDist = textSimStrategy.findDistance(firstItems[0], secondItems[0]);	
    	double lastNameDist = textSimStrategy.findDistance(firstItems[firstItems.length-1], 
    			secondItems[secondItems.length-1]);	
    	dist = firstNameDist * field.getPartWeights()[0] + lastNameDist * field.getPartWeights()[1]; 
    	return dist;
    }
    
    /**
     * @param field
     * @param firstAttr
     * @param secondAttr
     * @return
     * @throws IOException
     */
    private double streetAddressDistance(Field field, String firstAttr, String secondAttr) throws IOException {
    	double dist = 0;
    	String[] firstStreetCoponents = getStreetComponents(firstAttr);
    	String[] secondStreetCoponents = getStreetComponents(secondAttr);
    	dist = textSimStrategy.findDistance(firstStreetCoponents[0], secondStreetCoponents[0]) * field.getPartWeights()[0] + 
    			textSimStrategy.findDistance(firstStreetCoponents[1], secondStreetCoponents[1]) * field.getPartWeights()[1]; 
    	return dist;
    }
    
    /**
     * @param address
     * @return
     */
    private String[] getStreetComponents(String address) {
    	String baseAddress = "";
    	int pos;
    	String[] streeTypes = {"Street", "Avenue", "Road", "Boulevard"};
    	for (String streetType : streeTypes) {
        	pos = address.indexOf(streetType);
        	if (pos > 0) {
        		baseAddress = address.substring(0, pos) + streetType;
        		break;
        	}    		
    	}
    	String[] streetCoponents = new String[2];
    	pos = baseAddress.indexOf("\\s+");
    	streetCoponents[0] = baseAddress.substring(0, pos);
    	streetCoponents[1] = baseAddress.substring(pos).trim();
    	return streetCoponents;
    }
}    
    

