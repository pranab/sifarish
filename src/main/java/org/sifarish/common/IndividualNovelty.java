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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.mr.Transformer;
import org.chombo.transformer.AttributeTransformer;

/**
 * Per user item novelty. Novelty has an inverse relationship with user's engagement 
 * with an item. Input: userID, itemID, rating, rating distr
 * @author pranab
 *
 */
public class IndividualNovelty extends Transformer {
	
	@Override
	public int run(String[] args) throws Exception {
		return start("Individual novelty MR", IndividualNovelty.class, IndividualNovelty.NoveltyMapper.class,  args);
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class NoveltyMapper extends  Transformer.TransformerMapper {
		
        /* (non-Javadoc)
         * @see org.chombo.mr.Transformer.TransformerMapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	super.setup(context);
        	Configuration config = context.getConfiguration();
        	String strategy = config.get("inn.novelty.gen.strategy", "selfInformation");
        	int maxRating = config.getInt("inn.rating.scale", 100);
        	if (strategy.equals("selfInformation")) {
        		//based on rating distribution
        		 int engaementDistrScale = config.getInt("inn.engaement.distr.scale",  1000);
       		 	 registerTransformers(2, new Transformer.NullTransformer());
        		 registerTransformers(3, new IndividualNovelty.SelfInformation(engaementDistrScale, maxRating));
        	} else if  (strategy.equals("nonLinearInverse")) {
        		//based on rating
        		double param = config.getFloat("inn.quadratic.param", (float) 0.8);
       		 	registerTransformers(2, new IndividualNovelty.NonLinearInverse(maxRating, param));
      		 	registerTransformers(3, new Transformer.NullTransformer());
        	}
        }
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class SelfInformation extends AttributeTransformer {
		private double maxNovelty;
		private int maxRating;
		
		public SelfInformation(int engaementDistrScale, int maxRating) {
			maxNovelty = log2(engaementDistrScale);
			this.maxRating = maxRating;
		}
		
		private double log2(int val) {
			return Math.log(val) /  Math.log(2);
		}
		
		@Override
		public String[] tranform(String value) {
			//from rating distr to novelty
			int rating = Integer.parseInt(value);
			rating = rating == 0 ? 1 : rating;
			Integer novelty = (int)((1.0 - log2(rating) / maxNovelty) * maxRating);
			String[] transformed = new String[1];
			transformed[0] = novelty.toString();
			return transformed;
		}
	}

	/**
	 * Second degree polynomial
	 * @author pranab
	 *
	 */
	public static class NonLinearInverse  extends AttributeTransformer {
		private int maxRating; 
		private double k0, k1, k2;
		private int maxRatingInData;
		
		public NonLinearInverse(int maxRating, double param) {
			this.maxRating = maxRating;
			k0 = maxRating;
			k1 = -3 + 2 * param;
			k2 = 2 * (1 - param)  / maxRating;
		}
		public NonLinearInverse(int maxRating, double param, int maxRatingInData) {
			this(maxRating, param);
			this.maxRatingInData = maxRatingInData;
		}
		
		@Override
		public String[] tranform(String value) {
			//from rating to novelty
			int rating = Integer.parseInt(value);
			
			//scale it
			if (maxRatingInData > 0) {
				rating = (rating * maxRating) / maxRatingInData;
			}
			
			Integer novelty;
			if (rating == 0) {
				novelty = maxRating;
			} else if (rating == maxRating){
				novelty = 0;
			} else {
				novelty = (int)(k2 * rating * rating + k1 * rating + k0);
			}
			String[] transformed = new String[1];
			transformed[0] = novelty.toString();
			return transformed;
		}
		
	}	
	
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new IndividualNovelty(), args);
        System.exit(exitCode);
    }
	
}
