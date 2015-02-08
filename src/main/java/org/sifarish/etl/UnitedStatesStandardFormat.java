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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Standard format for US
 * @author pranab
 *
 */
public class UnitedStatesStandardFormat extends CountryStandardFormat {

    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#intializeStateCodes()
     */
    public void intializeStateCodes() {
    	stateCodes.put("alabama", "AL");
    	stateCodes.put("alaska", "AK");
    	stateCodes.put("arizona", "AZ");
    	stateCodes.put("arkansas", "AR");
    	stateCodes.put("california", "CA");
    	stateCodes.put("colorado", "CO");
    	stateCodes.put("connecticut", "CT");
    	stateCodes.put("delaware", "DE");
    	stateCodes.put("district of columbia", "DC");
    	stateCodes.put("florida", "FL");
    	stateCodes.put("georgia", "GA");
    	stateCodes.put("hawaii", "HI");
    	stateCodes.put("idaho", "ID");
    	stateCodes.put("illinois", "IL");
    	stateCodes.put("indiana", "IN");
    	stateCodes.put("iowa", "IA");
    	stateCodes.put("kansas", "KS");
    	stateCodes.put("kentucky", "KY");
    	stateCodes.put("louisiana", "LA");
    	stateCodes.put("maine", "ME");
    	stateCodes.put("maryland", "MD");
    	stateCodes.put("massachusetts", "MA");
    	stateCodes.put("michigan", "MI");
    	stateCodes.put("minnesota", "MN");
    	stateCodes.put("mississippi", "MS");
    	stateCodes.put("missouri", "MO");
    	stateCodes.put("montana", "MT");
    	stateCodes.put("nebraska", "NE");
    	stateCodes.put("new hampshire", "NH");
    	stateCodes.put("new jersey", "NJ");
    	stateCodes.put("new mexico", "NM");
    	stateCodes.put("new york", "NY");
    	stateCodes.put("north carolina", "NC");
    	stateCodes.put("north dakota", "ND");
    	stateCodes.put("ohio", "OH");
    	stateCodes.put("oklahoma", "OK");
    	stateCodes.put("oregon", "OR");
    	stateCodes.put("pennsylvania", "PA");
    	stateCodes.put("rhode island", "RI");
    	stateCodes.put("south carolina", "SC");
    	stateCodes.put("south dakota", "SD");
    	stateCodes.put("tennessee", "TN");
    	stateCodes.put("texas", "TX");
    	stateCodes.put("utah", "UT");
    	stateCodes.put("vermont", "VT");
    	stateCodes.put("virginia", "VA");
    	stateCodes.put("washington", "WA");
    	stateCodes.put("west virginia", "WV");
    	stateCodes.put("wisconsin", "WI");
    	stateCodes.put("wyoming", "WY");
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
			throw new IllegalArgumentException("invalid phone number format");
		}
    	return item;
    }

    /* (non-Javadoc)
     * @see org.sifarish.etl.CountryStandardFormat#stateFormat(java.lang.String)
     */
    public String stateFormat(String item) {
    	if (item.length() == 2) {
    		item = item.toUpperCase();
    		if (!stateCodes.containsValue(item)) {
    			throw new IllegalArgumentException("invalid state code");
    		}
    	} else {
    		item = item.toLowerCase();
    		item = stateCodes.get(item);
    		if (null == item) {
    			throw new IllegalArgumentException("invalid state name");
    		}
    	}
    	
    	return item;
    }
    
}
