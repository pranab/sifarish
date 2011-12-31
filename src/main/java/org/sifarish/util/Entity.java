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

import java.util.ArrayList;
import java.util.List;

public class Entity {
	private String name;
	private int type;
	private int fieldCount;
	private List<Field> fields;
	private List<FieldExtractor> fieldExtractors;
	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public int getFieldCount() {
		return fieldCount;
	}
	public void setFieldCount(int fieldCount) {
		this.fieldCount = fieldCount;
	}

	public List<Field> getFields() {
		return fields;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}
	
	public List<FieldExtractor> getFieldExtractors() {
		return fieldExtractors;
	}
	public void setFieldExtractors(List<FieldExtractor> fieldExtractors) {
		this.fieldExtractors = fieldExtractors;
	}

	public Field getIdField() {
		Field field = null;
		for (Field thisField : fields) {
			if (thisField.isId()){
				field = thisField;
				break;
			}
		}
		return field;
	}
	
	public List<FieldExtractor> getExtractorsForField(int ordinal){
		List<FieldExtractor> extractors =  new ArrayList<FieldExtractor>();
		
		for (FieldExtractor extractor :  fieldExtractors) {
			if (extractor.allSrcFields() || extractor.getSrcOrdinal() == ordinal){
				extractors.add(extractor);
			} 
		}
		
		return extractors;
	}
	
	public boolean isRetainedField(int ordinal){
		boolean retained = false;
		for (Field field : fields){
			if (field.getOrdinal() == ordinal){
				retained = true;
				break;
			}
		}
		return retained;
	}
	
	
}
