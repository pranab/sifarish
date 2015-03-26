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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Entity
 * @author pranab
 *
 */
public class Entity implements Serializable {
	private String name;
	private int type;
	private int fieldCount;
	private String filePrefix;
	private List<Field> fields;
	private List<FieldExtractor> fieldExtractors = new ArrayList<FieldExtractor>();
	
	
	/**
	 * @return
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}
	/**
	 * @return
	 */
	public int getType() {
		return type;
	}
	/**
	 * @param type
	 */
	public void setType(int type) {
		this.type = type;
	}
	/**
	 * @return
	 */
	public int getFieldCount() {
		return fieldCount;
	}
	/**
	 * @param fieldCount
	 */
	public void setFieldCount(int fieldCount) {
		this.fieldCount = fieldCount;
	}

	/**
	 * @return
	 */
	public String getFilePrefix() {
		return filePrefix;
	}
	/**
	 * @param filePrefix
	 */
	public void setFilePrefix(String filePrefix) {
		this.filePrefix = filePrefix;
	}
	/**
	 * @return
	 */
	public List<Field> getFields() {
		return fields;
	}

	/**
	 * @param fields
	 */
	public void setFields(List<Field> fields) {
		this.fields = fields;
	}
	
	/**
	 * @return
	 */
	public List<FieldExtractor> getFieldExtractors() {
		return fieldExtractors;
	}
	/**
	 * @param fieldExtractors
	 */
	public void setFieldExtractors(List<FieldExtractor> fieldExtractors) {
		this.fieldExtractors = fieldExtractors;
	}

	/**
	 * @return
	 */
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
	
	/**
	 * @return
	 */
	public Field getClassAttributeField() {
		Field field = null;
		for (Field thisField : fields) {
			if (thisField.isClassAttribute()){
				field = thisField;
				break;
			}
		}
		return field;
	}
	
	/**
	 * @param ordinal
	 * @return
	 */
	public List<FieldExtractor> getExtractorsForField(int ordinal){
		List<FieldExtractor> extractors =  new ArrayList<FieldExtractor>();
		
		for (FieldExtractor extractor :  fieldExtractors) {
			if (extractor.allSrcFields() || extractor.getSrcOrdinal() == ordinal){
				extractors.add(extractor);
			} 
		}
		
		return extractors;
	}
	
	/**
	 * @param ordinal
	 * @return
	 */
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
	
	/**
	 * @param ordinal
	 * @return
	 */
	public Field getFieldByOrdinal(int ordinal) {
		Field field = null;
		for (Field thisField : fields){
			if (thisField.getOrdinal() == ordinal){
				field = thisField;
				break;
			}
		}
		return field;
	}
	
}
