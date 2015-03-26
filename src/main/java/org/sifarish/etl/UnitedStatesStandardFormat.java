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

package org.sifarish.etl;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.sifarish.feature.DynamicAttrSimilarityStrategy;
import org.sifarish.util.Field;

/**
 * Standard format for US
 * @author pranab
 *
 */
public class UnitedStatesStandardFormat extends CountryStandardFormat {
	private StructuredTextNormalizer textNormalizer;
	
    public UnitedStatesStandardFormat(StructuredTextNormalizer textNormalizer) {
		super();
		this.textNormalizer = textNormalizer;
	}

	/* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#intializeStateCodes()
     */
    public void intializeStateCodes() {
    }

    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#caseFormat(java.lang.String, java.lang.String)
     */
    public String caseFormat(String item, String format) {
    	String[] tokens = item.split("\\s+");
    	for (int i = 0; i < tokens.length; ++i) {
    		if (format.equals("lowerCase")) {
    			tokens[i] = tokens[i].toLowerCase(); 
    		} else if (format.equals("upperCase")) {
    			tokens[i] = tokens[i].toUpperCase(); 
    		} else if (format.equals("capitalize")) {
    			tokens[i] = StringUtils.capitalize(tokens[i].toLowerCase());
    		} else {
    			throw new IllegalArgumentException("invalid case format");
    		}
    	}
    	
    	return org.chombo.util.Utility.join(tokens, "");
    }

    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#phoneNumFormat(java.lang.String, java.lang.String)
     */
    public String phoneNumFormat(String item, String format) {
		item = item.replaceAll("^\\d", "");
    	if (format.equals("compact")) {
    	} else if (format.equals("areaCodeParen")) {
    		item = "(" + item.substring(0, 3) + ")" + item.substring(3);
    	} else if (format.equals("spaceSep")) {
    		item = item.substring(0, 3) + " " + item.substring(3,6) + " " + item.substring(6);
    	} else {
		}
    	return item;
    }

    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#stateFormat(java.lang.String)
     */
    public String stateFormat(String item) throws IOException {
    	return stateFormat(item, false, null, 0);
    }
    
    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#stateFormat(java.lang.String, boolean, org.sifarish.feature.DynamicAttrSimilarityStrategy, double)
     */
    public String stateFormat(String item, boolean fuzzyMatch, DynamicAttrSimilarityStrategy textSimStrategy, 
    	double minDist) throws IOException {
    	String newItem = item;
    	TextFieldTokenNormalizer tokenNormalizer = 
    			textNormalizer.findTokenNormalizer(Field.TEXT_TYPE_STATE);
    	if (newItem.length() == 2) {
    		newItem = newItem.toUpperCase();
        	if (fuzzyMatch && !tokenNormalizer.containsNormalize(newItem)) {
        		//try fuzzy matching
	    		Pair<String, Double>  match = tokenNormalizer.fuzzymatchWithNormalized(newItem, textSimStrategy);
	    		if (match.getRight() <= minDist) {
	    			newItem = match.getLeft();
	    		}
        	}
    	} else {
        	newItem = tokenNormalizer.normalize(newItem);
        	if (fuzzyMatch && newItem.equals(item)) {
        		//try fuzzy matching
        		Pair<String, Double>  match = tokenNormalizer.fuzzymatchWithUnnormalized(newItem, textSimStrategy);
        		if (match.getRight() <= minDist) {
        			newItem = match.getLeft();
        			newItem = tokenNormalizer.normalize(newItem);
        		}
        	}
    	}
    	
    	return newItem;
    }

    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#streetAddressFormat(java.lang.String)
     */
    public String streetAddressFormat(String item) throws IOException {
    	String newItem = streetAddressOneFormat(item);
    	return streetAddressTwoFormat(newItem);
    }   

    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#streetAddressOneFormat(java.lang.String)
     */
    public String streetAddressOneFormat(String item) throws IOException {
    	return streetAddressOneFormat(item, false, null, 0);
    }   

    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#streetAddressOneFormat(java.lang.String, boolean, org.sifarish.feature.DynamicAttrSimilarityStrategy, double)
     */
    public String streetAddressOneFormat(String item, boolean fuzzyMatch, DynamicAttrSimilarityStrategy textSimStrategy, 
        	double minDist) throws IOException {
    	TextFieldTokenNormalizer tokenNormalizer = 
    			textNormalizer.findTokenNormalizer(Field.TEXT_TYPE_STREET_ADDRESS_ONE);
    	String newItem = tokenNormalizer.normalize(item);
    	
    	if (fuzzyMatch && newItem.equals(item)) {
    		String[] elements = newItem.split("\\s+");
    		String streetType = elements[elements.length -1];
        	tokenNormalizer = textNormalizer.findTokenNormalizer(Field.TEXT_TYPE_STREET_TYPE);
        	if (!tokenNormalizer.containsNormalize(streetType)) {
	    		//try fuzzy matching
        		Pair<Boolean, String> fuzzyMatched = fuzyyMatchComponent(streetType,  tokenNormalizer, 
        	    		textSimStrategy, minDist);
	    		if (fuzzyMatched.getLeft()) {
		    		StringBuilder stBld = new StringBuilder(elements[0]);
		    		for (int i = 1; i < elements.length-1; ++i) {
		    			if (i == elements.length-1) {
		    				stBld.append(" ").append(fuzzyMatched.getRight());
		    			} else {
		    				stBld.append(" ").append(elements[i]);
		    			}
		    		}
		    		newItem = stBld.toString();
	    		}
        	}
    	}
    	return newItem;
    }   
    
    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#streetAddressTwoFormat(java.lang.String)
     */
    public String streetAddressTwoFormat(String item) throws IOException  {
    	return streetAddressTwoFormat(item, false, null, 0);
    }   

    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#streetAddressTwoFormat(java.lang.String, boolean, org.sifarish.feature.DynamicAttrSimilarityStrategy, double)
     */
    public String streetAddressTwoFormat(String item, boolean fuzzyMatch, DynamicAttrSimilarityStrategy textSimStrategy, 
        	double minDist) throws IOException {
    	TextFieldTokenNormalizer tokenNormalizer = 
    			textNormalizer.findTokenNormalizer(Field.TEXT_TYPE_STREET_ADDRESS_TWO);
    	String newItem = tokenNormalizer.normalize(item);
    	
    	if (fuzzyMatch && newItem.equals(item)) {
    		String[] elements = newItem.split("\\s+");
    		String unitType = elements[0];
        	if (!tokenNormalizer.containsNormalize(unitType)) {
	    		//try fuzzy matching
        		Pair<Boolean, String> fuzzyMatched = fuzyyMatchComponent(unitType,  tokenNormalizer, 
        	    		textSimStrategy, minDist);
	    		if (fuzzyMatched.getLeft()) {
		    		StringBuilder stBld = new StringBuilder(fuzzyMatched.getRight());
		    		for (int i = 1; i < elements.length-1; ++i) {
		    			stBld.append(" ").append(elements[i]);
		    		}
		    		newItem = stBld.toString();
	    		}
        	}
    	}
    	
    	return newItem;
    }   

    /**
     * Whole address in one field
     * @param item
     * @return
     */
    public String addressFormat(String item) {
    	TextFieldTokenNormalizer tokenNormalizer = 
    			textNormalizer.findTokenNormalizer(Field.TEXT_TYPE_STREET_ADDRESS_ONE);
    	String newItem = tokenNormalizer.normalize(item);
    	tokenNormalizer = textNormalizer.findTokenNormalizer(Field.TEXT_TYPE_STREET_ADDRESS_TWO);
    	newItem = tokenNormalizer.normalize(newItem);
    	
    	String[] lines = newItem.split("\\n");
    	
    	//break address line 2
    	if (lines.length == 3) {
    		lines[1] = breakAddressLine(lines[1], "Apartment");
    		lines[1] = breakAddressLine(lines[1], "Suite");
    		newItem = lines[0] + "\\n" + lines[1] + "\\n" + lines[2];
    	}
    	return newItem;
    }
    
    private String breakAddressLine(String line, String unit) {
    	String newLine = line;
		if (line.contains(unit)) {
			String[] subLines = line.split(unit);
			newLine = subLines[0] + "\\n" + unit + subLines[1];
		}
    	return newLine;
    }
    
}
