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
import org.chombo.redis.RedisCache;

/**
 * Item novelty aggregated over all users. Novelty has an inverse relationship with user's engagement 
 * with an item. Input:  itemID, rating, rating distr
 * @author pranab
 *
 */
public class GlobalNovelty  extends Transformer {

	@Override
	public int run(String[] args) throws Exception {
		return start("Global novelty MR", GlobalNovelty.class, GlobalNovelty.NoveltyMapper.class,  args);
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
        	String strategy = config.get("novelty.gen.strategy", "selfInformation");
        	int maxRating = config.getInt("rating.scale", 100);
        	if (strategy.equals("selfInformation")) {
        		//based on rating distribution
        		RedisCache cache = RedisCache.createRedisCache(config, "ch");
				String countMaxValueKeyPrefix = config.get("count.max.value.key.prefix");
        		int engaementDistrScale = cache.getIntMax(countMaxValueKeyPrefix);
       		 	registerTransformers(1, new Transformer.NullTransformer());
        		registerTransformers(2, new IndividualNovelty.SelfInformation(engaementDistrScale, maxRating));
        	} else if  (strategy.equals("nonLinearInverse")) {
        		//based on rating
        		double param = config.getFloat("quadratic.param", (float) 0.8);
       		 	registerTransformers(1, new IndividualNovelty.NonLinearInverse(maxRating, param));
      		 	registerTransformers(2, new Transformer.NullTransformer());
        	}
        }
	}

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GlobalNovelty(), args);
        System.exit(exitCode);
    }
	
}
