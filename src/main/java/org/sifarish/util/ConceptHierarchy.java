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

import java.util.List;


/**
 * Concept hierarchy
 * @author pranab
 *
 */
public class ConceptHierarchy {
	private List<Concept> concepts;

	public List<Concept> getConcepts() {
		return concepts;
	}

	public void setConcepts(List<Concept> concepts) {
		this.concepts = concepts;
	}

	public String findParent(String child){
		String parent = null;
		for (Concept concept : concepts){
			parent = concept.findParent(child);
			if (null != parent){
				break;
			}
		}
		
		return parent;
	}
	
	public static class Concept {
		private String parent;
		private List<String> children;
		
		public String getParent() {
			return parent;
		}
		public void setParent(String parent) {
			this.parent = parent;
		}
		public List<String> getChildren() {
			return children;
		}
		public void setChildren(List<String> children) {
			this.children = children;
		}
		
		public String findParent(String child){
			String parent = children.contains(child) ? this.parent : null; 
			return parent;
		}
	}
}
