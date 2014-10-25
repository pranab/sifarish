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

import java.util.HashMap;
import java.util.Map;

/**
 * Country specific formats for different kinds of structured text data
 * @author pranab
 *
 */
public abstract class CountryStandardFormat {
    protected Map<String, String> stateCodes = new HashMap<String, String>();
    
    /**
     * @param country
     * @return
     */
    public static CountryStandardFormat createCountryStandardFormat(String country) {
    	CountryStandardFormat countryFormat = null;
    	if (country.equals("United States")) {
    		countryFormat = new UnitedStatesStandardFormat();
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
    public abstract String stateFormat(String item);
}
