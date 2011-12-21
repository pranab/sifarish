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

public class SingleTypeSchema {
	private Entity entity;
	private String distAlgorithm;
	private double minkowskiParam;
	private double numericDiffThreshold;
	private String missingValueHandler = "default";
	private int partitioningColumn = -1;
	
	public Entity getEntity() {
		return entity;
	}
	public void setEntity(Entity entity) {
		this.entity = entity;
	}
	public String getDistAlgorithm() {
		return distAlgorithm;
	}
	public void setDistAlgorithm(String distAlgorithm) {
		this.distAlgorithm = distAlgorithm;
	}
	public double getMinkowskiParam() {
		return minkowskiParam;
	}
	public void setMinkowskiParam(double minkowskiParam) {
		this.minkowskiParam = minkowskiParam;
	}
	public double getNumericDiffThreshold() {
		return numericDiffThreshold;
	}
	public void setNumericDiffThreshold(double numericDiffThreshold) {
		this.numericDiffThreshold = numericDiffThreshold;
	}
	public String getMissingValueHandler() {
		return missingValueHandler;
	}
	public void setMissingValueHandler(String missingValueHandler) {
		this.missingValueHandler = missingValueHandler;
	}
	public int getPartitioningColumn() {
		return partitioningColumn;
	}
	public void setPartitioningColumn(int partitioningColumn) {
		this.partitioningColumn = partitioningColumn;
	}

}
