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

import java.util.HashSet;
import java.util.Set;

import org.chombo.util.Pair;
import org.sifarish.feature.SingleTypeSchema;

/**
 * Defines profile for various attributes. For numerical attribute it's a range. For
 * categorical attribute it's a set. For text, it's just the text.
 * @author pranab
 *
 */
public class MatchingProfile {
	private Object[] profile;
	
	/**
	 * @param line
	 * @param fieldDelim
	 * @param subFieldDelim
	 * @param schema
	 */
	public MatchingProfile(String line, String fieldDelim, String subFieldDelim, SingleTypeSchema schema) {
		String[] record = line.split(fieldDelim);
		profile = new Object[record.length];
		for (int i = 0; i < profile.length; ++i) {
			profile[i] = null;
		}
		
		//build profile
		for (Field field :  schema.getEntity().getFields()) {
			
			String  dataType =   field.getDataType();
			int ord = field.getOrdinal();
			String[] items = record[ord].split(subFieldDelim);
			if (field.isId()) {
				profile[ord] = record[ord];
			} else if (dataType.equals(Field.DATA_TYPE_INT)) {
				if (2 != items.length) {
					throw new IllegalStateException("numerical profile attribute has only range");
				}
				Pair<Integer, Integer> range = new Pair<Integer, Integer>(Integer.parseInt(items[0]), 
						Integer.parseInt(items[1]));
				profile[ord] = range;
			} else if (dataType.equals(Field.DATA_TYPE_DOUBLE)) {
				if (2 != items.length) {
					throw new IllegalStateException("numerical profile attribute has only range");
				}
				Pair<Double, Double> range = new Pair<Double, Double>(Double.parseDouble(items[0]), 
						Double.parseDouble(items[1]));
				profile[ord] = range;
			} else if (dataType.equals(Field.DATA_TYPE_CATEGORICAL)) {
				Set<String> values = new HashSet<String>();
				for (String value : items) {
					values.add(value);
				}
				profile[ord] = values;
			} else if (dataType.equals(Field.DATA_TYPE_TEXT)) {
				profile[ord] = line;
			} else {
				throw new IllegalStateException("unsupported data type");
			}
		} 
	}
	
	/**
	 * @param fieldOrd
	 * @return
	 */
	public String getId(int fieldOrd) {
		return (String)profile[fieldOrd];
	}
	
	/**
	 * @param fieldOrd
	 * @return
	 */
	public Pair<Integer, Integer> getIntRange(int fieldOrd) {
		return (Pair<Integer, Integer>)profile[fieldOrd];
	}
	
	/**
	 * @param fieldOrd
	 * @return
	 */
	public Pair<Double, Double> getDoubleRange(int fieldOrd) {
		return (Pair<Double, Double>)profile[fieldOrd];
	}
	
	/**
	 * @param fieldOrd
	 * @return
	 */
	public Set<String> getCategoricalSet(int fieldOrd) {
		return (Set<String>)profile[fieldOrd];
	}
	
	/**
	 * @param fieldOrd
	 * @return
	 */
	public String getText(int fieldOrd) {
		return (String)profile[fieldOrd];
	}
}
