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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * RDF based semantic similarity using Apache Jena
 * @author pranab
 * 
 */
public class ResourceDescribedEntity extends TaggedEntity {
	private Model model;
    private static final Logger LOG = Logger.getLogger(ResourceDescribedEntity.class);

	@Override
	public boolean isResultCorrelation() {
		return false;
	}

	@Override
	public int match(TaggedEntity other) throws IOException {
		String thisTag = getTag();
		String thatTag = other.getTag();
		int dist = 0;
		Resource matchingResource = null;
		
		if (!thisTag.equals(thatTag)) {
			//find intersections in RDF graph
			List<ResourceTraversed> intersections = match(thisTag, thatTag);
			
			//find the intersection with min distance
			dist = Integer.MAX_VALUE;
			for (ResourceTraversed resTrav :  intersections) {
				if (resTrav.getDistance() < dist) {
					dist = resTrav.getDistance();
					matchingResource = resTrav.getResource();
				}
			}
		} else {
			matchingResource = ResourceFactory.createResource(thisTag);
		}
		
		matchingContext = matchingResource != null ? resourceToString(matchingResource) : "";
		
		LOG.debug("dist:" + dist);
		return dist;
	}
	
	/**
	 * @param resource
	 * @return
	 */
	private String resourceToString(Resource resource) {
		StringBuilder stBld = new StringBuilder();
		StmtIterator iter = model.listStatements(resource, (Property)null, (RDFNode)null);
		while (iter.hasNext()) {
		    Statement stmt      = iter.nextStatement();  
		    Resource  subject   = stmt.getSubject();     
		    Property  predicate = stmt.getPredicate();   
		    RDFNode   object    = stmt.getObject();
		    stBld.append("[").append(subject.toString()).append("  ").append(predicate.toString()).append("  ").
		    	append(object.toString()).append("]");
		}		
		
		return stBld.toString();
	}
	
	/**
	 * @throws IOException
	 */
	private void loadModel() throws IOException {
		if (null == model) {
			FileSystem dfs = FileSystem.get((Configuration)params.get("config"));
            Path src = new Path((String)params.get("semantic.rdf.modelFilePath"));
            FSDataInputStream fs = dfs.open(src);			
            
            model = ModelFactory.createDefaultModel();	
            model.read(fs, "");	
		}
	}

	
	/**
	 * @param modelFilePath
	 * @param firstResource
	 * @param secondResource
	 */
	public List<ResourceTraversed>  match( String firstResource, String secondResource) {
		LOG.debug("firstResource:" + firstResource + " secondResource:" + secondResource );
		
		List<ResourceTraversed> firstSearchRes = search( firstResource);
		List<ResourceTraversed> secondSearchRes = search( secondResource);
		
		//find intersections
		List<ResourceTraversed> intersections = new ArrayList<ResourceTraversed>();
		for (ResourceTraversed firstRsrc : firstSearchRes) {
			for (ResourceTraversed secondRsrc : secondSearchRes) {
				if (firstRsrc.getResource().equals(secondRsrc.getResource())) {
					ResourceTraversed intersection = new ResourceTraversed(firstRsrc.getResource(), 
							firstRsrc.getDistance() + secondRsrc.getDistance());
					intersections.add(intersection);
				}
			}
		}
		
		LOG.debug("num of intersections:" + intersections.size());
		return intersections;
	}
	

	/**
	 * @param modelFilePath
	 * @param uriResource
	 */
	public List<ResourceTraversed> search(String uriResource) {
		Resource resource = ResourceFactory.createResource(uriResource);
		List<ResourceTraversed> resourcesTraversed = new ArrayList<ResourceTraversed>();
		searchOntology(resource, resourcesTraversed, 0);
		LOG.debug("next search - uriResource:" + uriResource + " num of paths:" + resourcesTraversed.size());
	    return resourcesTraversed;
	}
		
	
	/**
	 * @param resource
	 * @param resourcesTraversed
	 */
	private void searchOntology(Resource resource, List<ResourceTraversed> resourcesTraversed, int distance) {
		resourcesTraversed.add(new ResourceTraversed(resource, distance));
		StmtIterator iter = model.listStatements(resource, (Property)null, (RDFNode)null);
		while (iter.hasNext()) {
		    Statement stmt      = iter.nextStatement();  
		    Resource  subject   = stmt.getSubject();     
		    Property  predicate = stmt.getPredicate();   
		    RDFNode   object    = stmt.getObject();   
		       
		    //LOG.debug("next resource:" + object.toString());		    
		    if (object instanceof Resource) {
		    	//if another RDF node, recurse
		    	searchOntology((Resource)object, resourcesTraversed, distance+1);
		    }
		}		
	}
	
	@Override
	public void initialize(Map<String, Object> params) throws IOException {
		this.params = params;
		loadModel() ;		
		Configuration conf = (Configuration)params.get("config");
        if (conf.getBoolean("debug.on", false)) {
         	LOG.setLevel(Level.DEBUG);
        }
		
	}	

	
	/**
	 * @author pranab
	 *
	 */
	private static class ResourceTraversed {
		private Resource resource;
		private int distance;
		
		public ResourceTraversed(Resource resource, int distance) {
			this.resource = resource;
			this.distance = distance;
		}

		public Resource getResource() {
			return resource;
		}

		public int getDistance() {
			return distance;
		}

		public String toString() {
			return "resource:" + resource.toString() + " distance: " + distance;
		}
	}


}
