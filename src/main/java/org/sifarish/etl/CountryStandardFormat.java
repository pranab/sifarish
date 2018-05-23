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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.sifarish.feature.DynamicAttrSimilarityStrategy;

/**
 * Country specific formats for different kinds of structured text data
 * @author pranab
 *
 */
public abstract class CountryStandardFormat {
    protected Map<String, String> stateCodes = new HashMap<String, String>();
    
	private static String fullName = "(\\w{2,})\\s+(\\w{2,})\\s+(\\w{2,})";
	private static String firstFullName = "(\\w{2,})\\s+(\\w{2,})";
	private static String firstFullMidIntialName = "(\\w{2,})\\s+(\\w{1})\\s+(\\w{2,})";
	private static String firstMidIntialName = "(\\w{1})\\s+(\\w{1})\\s+(\\w{2,})";
	private static String lastNameFirstFirstIntialName = "(\\w{2,}),\\s+(\\w{1})";
	private static String lastNameFirstFirstName = "(\\w{2,}),\\s+(\\w{2,})";
	private static String lastNameFirstFirstMidIntialName = "(\\w{2,}),\\s+(\\w{1})\\s+(\\w{1})";
	private static String lastNameFirstFirstMidName = "(\\w{2,}),\\s+(\\w{2,})\\s+(\\w{2,})";
	
	private static Pattern fullNamePattern = Pattern.compile(fullName);
	private static Pattern firstFullNamePattern = Pattern.compile(firstFullName);
	private static Pattern firstFullMidIntialNamePattern = Pattern.compile(firstFullMidIntialName);
	private static Pattern firstMidIntialNamePattern = Pattern.compile(firstMidIntialName);
	private static Pattern lastNameFirstFirstIntialNamePattern = Pattern.compile(lastNameFirstFirstIntialName);
	private static Pattern lastNameFirstFirstNamePattern = Pattern.compile(lastNameFirstFirstName);
	private static Pattern lastNameFirstFirstMidIntialNamePattern = Pattern.compile(lastNameFirstFirstMidIntialName);
	private static Pattern lastNameFirstFirstMidNamePattern = Pattern.compile(lastNameFirstFirstMidName);
    
    /**
     * @param country
     * @return
     */
    public static CountryStandardFormat createCountryStandardFormat(String country, StructuredTextNormalizer textNormalizer) {
    	CountryStandardFormat countryFormat;
    	if (country.equals("USA")) {
    		countryFormat = new UnitedStatesStandardFormat(textNormalizer);
    	} else {
    		throw new IllegalArgumentException("invalid country name");
    	}
    	
    	return countryFormat;
    }
    
    /**
     * 
     */
    public CountryStandardFormat() {
		super();
		intializeStateCodes();
	}

	/**
     * initializes state codes
     */
    public abstract void intializeStateCodes();
    
    /**
     * case based formatting
     * @param item
     * @param format
     * @return
     */
    public abstract String caseFormat(String item, String format);
    
    /**
     * phone number formatting
     * @param item
     * @param format
     * @return
     */
    public abstract String phoneNumFormat(String item, String format);

    /**
     * state name formatting
     * @param item
     * @return
     */
    public abstract String stateFormat(String item) throws IOException;
    
    /**
     * @param item
     * @param fuzzyMatch
     * @param textSimStrategy
     * @param minDist
     * @return
     * @throws IOException
     */
    public abstract String stateFormat(String item, boolean fuzzyMatch, DynamicAttrSimilarityStrategy textSimStrategy, 
        	double minDist) throws IOException;

    
    /**
     * @param item
     * @param format
     * @return
     */
    public String personNameFormat(String item) {
    	String firstFull = null;
    	String firstInitial = null;
    	String middleFull = null;
    	String middleInitial = null;
    	String last = null;
    	boolean matchFound = false;
    	
    	//first full,  mid full, last
    	Matcher matcher = fullNamePattern.matcher(item);
    	if (matcher.matches()) {
    		firstFull = matcher.group(1);
    		middleFull = matcher.group(2);
    		last = matcher.group(3);
    		matchFound = true;
    	}
    	
    	//first full, last
    	if (!matchFound) {
    		matcher = firstFullNamePattern.matcher(item);
        	if (matcher.matches()) {
        		firstFull = matcher.group(1);
        		last = matcher.group(2);
        		matchFound = true;
        	}    		
    	}
    	
    	//first full, middle initial,last
    	if (!matchFound) {
    		matcher = firstFullMidIntialNamePattern.matcher(item);
        	if (matcher.matches()) {
        		firstFull = matcher.group(1);
        		middleInitial = matcher.group(2);
        		last = matcher.group(3);
        		matchFound = true;
        	}    		
    	}
    	
    	//first , middle initial,last
    	if (!matchFound) {
    		matcher = firstMidIntialNamePattern.matcher(item);
        	if (matcher.matches()) {
        		firstInitial = matcher.group(1);
        		middleInitial = matcher.group(2);
        		last = matcher.group(3);
        		matchFound = true;
        	}    		
    	}
    	
    	//last , first initial
    	if (!matchFound) {
    		matcher = lastNameFirstFirstIntialNamePattern.matcher(item);
        	if (matcher.matches()) {
        		last = matcher.group(1);
        		firstInitial = matcher.group(2);
        		matchFound = true;
        	}    		
    	}

    	//last , first
    	if (!matchFound) {
    		matcher = lastNameFirstFirstNamePattern.matcher(item);
        	if (matcher.matches()) {
        		last = matcher.group(1);
        		firstFull = matcher.group(2);
        		matchFound = true;
        	}    		
    	}

    	//last , first initial, mid intial
    	if (!matchFound) {
    		matcher = lastNameFirstFirstMidIntialNamePattern.matcher(item);
        	if (matcher.matches()) {
        		last = matcher.group(1);
        		firstInitial = matcher.group(2);
        		middleInitial = matcher.group(3);
        		matchFound = true;
        	}    		
    	}

    	//last , first , mid 
    	if (!matchFound) {
    		matcher = lastNameFirstFirstMidNamePattern.matcher(item);
        	if (matcher.matches()) {
        		last = matcher.group(1);
        		firstFull = matcher.group(2);
        		middleFull = matcher.group(3);
        		matchFound = true;
        	}    		
    	}
    	
    	if (null != firstFull) {
    		firstFull = StringUtils.capitalize(firstFull.toLowerCase());
    	}
    	if (null != firstInitial) {
    		firstInitial = StringUtils.upperCase(firstInitial);
    	}
    	if (null != middleFull) {
    		middleFull = StringUtils.capitalize(middleFull.toLowerCase());
    	}
    	if (null != middleInitial) {
    		middleInitial = StringUtils.upperCase(middleInitial);
    	}
    	if (null != last) {
    		last = StringUtils.capitalize(last.toLowerCase());
    	}
    	
    	StringBuilder stBld = new StringBuilder();
    	if (null != firstFull) {
    		stBld.append(firstFull).append(" ");
    	} else if (null != firstInitial ) {
    		stBld.append(firstInitial).append(" ");
    	}
    	
    	if (null != middleFull) {
    		stBld.append(middleFull).append(" ");
    	} else if (null != middleInitial ) {
    		stBld.append(middleInitial).append(" ");
    	}
    	
    	if (null != last) {
    		stBld.append(last);
    	}   	
    	
    	return stBld.toString();
    }
    
    /**
     * @param item
     * @return
     */
    public abstract String streetAddressFormat(String item) throws IOException;
    
    /**
     * @param item
     * @return
     */
    public abstract String addressFormat(String item) throws IOException;
    
    /**
     * @param item
     * @return
     */
    public abstract String streetAddressOneFormat(String item) throws IOException;

    /**
     * @param item
     * @param fuzzyMatch
     * @param textSimStrategy
     * @param minDist
     * @return
     * @throws IOException
     */
    public abstract String streetAddressOneFormat(String item, boolean fuzzyMatch, DynamicAttrSimilarityStrategy textSimStrategy, 
        	double minDist) throws IOException;   
    
    /**
     * @param item
     * @return
     */
    public abstract String streetAddressTwoFormat(String item) throws IOException;
    
    /**
     * @param item
     * @param fuzzyMatch
     * @param textSimStrategy
     * @param minDist
     * @return
     * @throws IOException
     */
    public abstract  String streetAddressTwoFormat(String item, boolean fuzzyMatch, DynamicAttrSimilarityStrategy textSimStrategy, 
        	double minDist) throws IOException;
   
    /**
     * @param item
     * @param format
     * @return
     */
    public String emailFormat(String item, String format) {
    	String[] elements = item.split("@");
    	String name = elements[0];
		if (format.equals("lowerCase")) {
			name = name.toLowerCase(); 
		} else if (format.equals("upperCase")) {
			name = name.toUpperCase(); 
		} else if (format.equals("capitalize")) {
			name = StringUtils.capitalize(name.toLowerCase());
		} else {
			throw new IllegalArgumentException("invalid case format");
		}
    	
		return name + "@" + elements[1];
    }
    
    /**
     * @param item
     * @return
     */
    public String removePunctuations(String item) {
    	String newItem = item.replaceAll("\\.","");
    	newItem = item.replaceAll(",","");
    	return newItem;
    }
    
    /**
     * @param component
     * @param tokenNormalizer
     * @param textSimStrategy
     * @param minDist
     * @return
     * @throws IOException
     */
    protected Pair<Boolean, String> fuzyyMatchComponent(String component,  TextFieldTokenNormalizer tokenNormalizer, 
    		DynamicAttrSimilarityStrategy textSimStrategy, double minDist) throws IOException {
		String newComponent = component;
		boolean fuzzyMatched = false;
    	if (!tokenNormalizer.containsNormalize(component)) {
    		//try fuzzy matching
    		Pair<String, Double>  match = tokenNormalizer.fuzzymatchWithUnnormalized(component, textSimStrategy);
    		if (match.getRight() <= minDist) {
    			newComponent = match.getLeft();
    			newComponent = tokenNormalizer.normalize(newComponent);
    			fuzzyMatched = true;
    		} else {
    			match = tokenNormalizer.fuzzymatchWithNormalized(component, textSimStrategy);
        		if (match.getRight() <= minDist) {
        			newComponent = match.getLeft();
        			fuzzyMatched = true;
        		}
    		}
    	}
		return  new ImmutablePair<Boolean, String>(fuzzyMatched, newComponent);
    }

}
