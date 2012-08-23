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

package org.sifarish.common;

import org.chombo.util.Pair;

/**
 * @author pranab
 *
 */
public abstract class TaggedEntity extends Pair<String, String> {
	private String groupID;
	
	public TaggedEntity() {
	}
	
	/**
	 * @param entityID
	 * @param groupID
	 * @param tag
	 */
	public TaggedEntity(String entityID,  String groupID, String tag) {
		super(entityID, tag);
		this.groupID = groupID;
	}
	
	public String getEntityID() {
		return getLeft();
	}

	public void setEntityID(String entityID) {
		setLeft(entityID);
	}

	public String getTag() {
		return getRight();
	}

	public void setTag(String tag) {
		setRight(tag);
	}

	public String getGroupID() {
		return groupID;
	}

	public void setGroupID(String groupID) {
		this.groupID = groupID;
	}

	/**
	 * @param other
	 * @return
	 */
	public abstract int match(TaggedEntity other) ;

}
