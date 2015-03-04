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

/**
 * @author pranab
 *
 */
public class TextFieldTokenNormalizer {
	private String fieldType;
	private String[][] normalizers;
	
	/**
	 * @return
	 */
	public String getFieldType() {
		return fieldType;
	}
	/**
	 * @param fieldType
	 */
	public void setFieldType(String fieldType) {
		this.fieldType = fieldType;
	}
	/**
	 * @return
	 */
	public String[][] getNormalizers() {
		return normalizers;
	}
	/**
	 * @param normalizers
	 */
	public void setNormalizers(String[][] normalizers) {
		this.normalizers = normalizers;
	}
	
	/**
	 * @param item
	 * @return
	 */
	public String normalize(String item) {
		String newItem = item;
		for (String[] normalizer : normalizers) {
			newItem = newItem.replace(normalizer[0], normalizer[1]);
		}
		return newItem;
	}
	
	/**
	 * @param normalized
	 * @return
	 */
	public boolean containsNormalize(String normalized) {
		boolean contains = false;
		for (String[] normalizer : normalizers) {
			contains = normalizer[1].equals(normalized);
			if (contains)
				break;
		}		
		
		return contains;
	}
	
}
