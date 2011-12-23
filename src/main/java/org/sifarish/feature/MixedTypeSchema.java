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

import java.util.List;

import org.sifarish.util.Entity;
import org.sifarish.util.Field;

public class MixedTypeSchema  extends TypeSchema {
	private List<Entity> entities;
	private double srcNonMatchingTermWeight = 1.0;
	private double trgNonMatchingTermWeight = 1.0;
	
	public List<Entity> getEntities() {
		return entities;
	}

	public void setEntities(List<Entity> entities) {
		this.entities = entities;
	}
	
	public Entity getEntityBySize(int fieldCount) {
		Entity entity = null;
		for (Entity thisEntity : entities) {
			if (thisEntity.getFieldCount() == fieldCount){
				entity = thisEntity;
				break;
			}
		}
		
		return entity;
	}

	public Entity getEntityByType(int type) {
		Entity entity = null;
		for (Entity thisEntity : entities) {
			if (thisEntity.getType() == type){
				entity = thisEntity;
				break;
			}
		}
		
		return entity;
	}



	public double getSrcNonMatchingTermWeight() {
		return srcNonMatchingTermWeight;
	}

	public void setSrcNonMatchingTermWeight(double srcNonMatchingTermWeight) {
		this.srcNonMatchingTermWeight = srcNonMatchingTermWeight;
	}

	public double getTrgNonMatchingTermWeight() {
		return trgNonMatchingTermWeight;
	}

	public void setTrgNonMatchingTermWeight(double trgNonMatchingTermWeight) {
		this.trgNonMatchingTermWeight = trgNonMatchingTermWeight;
	}

	public double findCattegoricalDistance(String thisValue, String thatValue, int ordinal) {
		double distance = 1.0;
		
		Entity entity = entities.get(1);
		List<Field> fields = entity.getFields();
		for (Field field : fields) {
			if (ordinal == field.getOrdinal()) {
				distance = field.findDistance(thisValue, thatValue);
				break;
			}
		}
		
		return distance;
	}
	
	
	
	
}
