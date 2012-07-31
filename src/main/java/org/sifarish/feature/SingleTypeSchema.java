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
import org.sifarish.util.IDistanceStrategy;

/**
 * @author pranab
 *
 */
public class SingleTypeSchema  extends TypeSchema {
	private Entity entity;
	private int partitioningColumn = -1;
	
	/**
	 * @return
	 */
	public Entity getEntity() {
		return entity;
	}
	/**
	 * @param entity
	 */
	public void setEntity(Entity entity) {
		this.entity = entity;
	}
	/**
	 * @return
	 */
	public int getPartitioningColumn() {
		return partitioningColumn;
	}
	/**
	 * @param partitioningColumn
	 */
	public void setPartitioningColumn(int partitioningColumn) {
		this.partitioningColumn = partitioningColumn;
	}

	/**
	 * Process structured fields
	 */
	public void processStructuredFields() {
		for (Field field : entity.getFields()) {
			String distAlgorithm = field.getDistAlgorithm();
			if (null != distAlgorithm) {
				IDistanceStrategy distStrategy = null;
				if (distAlgorithm.equals("euclidean")) {
					distStrategy = new EuclideanDistance(1);
				} else if (distAlgorithm.equals("manhattan")) {
					distStrategy = new ManhattanDistance(1);
				}		
				field.setDistStrategy(distStrategy);
			}
		}
	}
}
