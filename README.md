## Introduction

Sifarish is a suite of recommendation engines implementaed on Hadoop and Storm. Various 
algorithms, including  feature similarity based recommendation and collaborative 
filtering based recommendation using social rating data will be available

## Blogs

The following blogs of mine are good source of details of sifarish. These are the only source
of detail documentation

* http://pkghosh.wordpress.com/2011/10/26/similarity-based-recommendation-basics/
* http://pkghosh.wordpress.com/2011/11/28/similarity-based-recommendation-hadoop-way/
* http://pkghosh.wordpress.com/2011/12/15/similarity-based-recommendation-text-analytic/
* http://pkghosh.wordpress.com/2012/04/21/socially-accepted-recommendation/
* http://pkghosh.wordpress.com/2010/10/19/recommendation-engine-powered-by-hadoop-part-1/
* http://pkghosh.wordpress.com/2010/10/31/recommendation-engine-powered-by-hadoop-part-2/
* http://pkghosh.wordpress.com/2012/12/31/get-social-with-pearson-correlation/
* http://pkghosh.wordpress.com/2012/09/03/from-item-correlation-to-rating-prediction/
* http://pkghosh.wordpress.com/2014/02/10/from-explicit-user-engagement-to-implicit-product-rating/
* http://pkghosh.wordpress.com/2014/04/14/making-recommendations-in-real-time/

## Content Similarity Based Recommendation

In the absence of social rating data, the only options is a feature similarity 
based recommendation. Similarity is calculated based on distance between entities 
in a multi dimensional feature space. Some examples are - recommending jobs based 
on user's resume - recommending products based on user profile. These
solutions are known as content based recommendation, because it's based innate 
features of some entity.

There are two different solutions as follows
1. Similarity between entities of different types (e.g. user profile and product)
2. Similarity between entities of same type (e.g. product)

Attribute meta data is defined in a json file. Both entities need not have the 
same set of attributes. Mapping between attributes values from one entity to 
the other can be defined in the config file.

The data type supported are numerical (integer), categorical, text. The distance algorithms 
can be chosed to be euclidian, manhattan or minkowski. The default algorithm is euclidian. 
The distancs between different atrributes of different types are combined to find distance between 
two entity instances. Different weights can be assigned to the attributes to control the relative 
importance of different attributes.


## Social Interaction Data Based Recommendation

These solutions are based user behavior data with respect to some product 
or service. these algorithms are also known as collaborative filtering.  

User behavior data is defined in terms of some explicit rating by user 
or it's derived from user  behavior in the site. The essential  input to all these algorithms 
is a matrix of user and items. The value for a cell could be the ratingas an integer. It could 
also be boolean,  if the user's interest in an item is expressed as a boolean


## Cold Starting Recommenders

These solutions are used when enough social data is not avaialable. 

1. If data contains text attributes, use TextAnalyzer MR to convert text to token stream 
   using lucene
2. Find similar items based on user profile. Use DiffTypeSimilarity MR
3. Use TopMatches MR to find top n matches for a profile


## Warm Starting Recommenders

When limited amount of user behavior data is available, these solutuions are appropriate

1. If data contains text attributes, use TextAnalyzer MR to convert text to token stream 
   using lucene
2. Find similar items by pairing items with one another using SameTypeSimilarity MR
3. Use TopMatches MR to find top n matches for a product


## Recommenders with Fully engaged Users

When significant of user behavior data is available, these soltions can be used. In 
the order of  complexity, the choices are as follows. They are all based on social data

There two phases for collaborative filetering based recommendation using social data
1. Find correlation between items 2. Predict rating based on items alreadyv rated and 
result of 1

The process involved running map reduce jobs. Some of them are optional. Please refer to the 
tutorial document tutorial.txt


## Real Time Recommendation

Recommendations can be made real time based on user's current behavior in a pre defined time
window. The solution is based on Storm, although Hadoop gets used to compute item correlation
matrix from historical user behavior data.

## Complex Attributes

There is  suppoort for structured fields e.g., Location, Time Window, Categorized Item, Product etc. 
These provide contextual dimensions to recommendation. They are particularly relevant for recommendation
in the mobile space

## Diversilty, Novelty 

Based on recent work in this area, I am working on implementing some  algorithms to introduce  
diversity and novelty in recommendation

## Facted Match

For content based recommendation, faceted match is supported as faceted search in Solr.
Faceted fields are specified through a configuration parameter

## Dithering
Dithering effectively handles the problem users usually not browsing the first few items
in a list. The dithering process shuffles the list little bit, every time recommended items 
are presented to the user.
 
## Getting started

Please use the tutorial.txt file in the resource directory for batch mode recommendation 
processing. For real time recommendation please use the tutorial document there is a separate
tutorial document realtime\_recommendation\_tutorial.txt








