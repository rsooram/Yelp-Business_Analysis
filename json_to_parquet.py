import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Subset_code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

# TO-DO
business_schema = types.StructType([ 
    types.StructField('business_id', types.StringType(), True),
    types.StructField('name', types.StringType(), True),
    types.StructField('neighborhood', types.StringType(), True),
    types.StructField('address', types.StringType(), True),
    types.StructField('city', types.StringType(), True),
    types.StructField('state', types.StringType(), True),
    types.StructField('postal code', types.StringType(), True),
    types.StructField('latitude', types.FloatType(), True),
    types.StructField('longtitude', types.FloatType(), True),
    types.StructField('stars', types.FloatType(), True),
    types.StructField('review_count', types.IntegerType(), True),
    types.StructField('is_open', types.IntegerType(), True),
    types.StructField('attributes', types.StructType(), True),
    types.StructField('categories', types.StringType(), True),
    types.StructField('hours', types.StructType(), True)])

review_schema = types.StructType([ 
    types.StructField('review_id', types.StringType(), True),
    types.StructField('user_id', types.StringType(), True),
    types.StructField('business_id', types.StringType(), True),
    types.StructField('stars', types.IntegerType(), True),
    types.StructField('date', types.StringType(), True),
    types.StructField('text', types.StringType(), True),
    types.StructField('useful', types.IntegerType(), True),
    types.StructField('funny', types.IntegerType(), True),
    types.StructField('cool', types.IntegerType(), True)])

user_schema = types.StructType([ 
    types.StructField('user_id', types.StringType(), True),
    types.StructField('name', types.StringType(), True),
    types.StructField('review_count', types.IntegerType(), True),
    types.StructField('yelping_since', types.StringType(), True),
    types.StructField('friends', types.StringType(), True),
    types.StructField('useful', types.IntegerType(), True),
    types.StructField('funny', types.IntegerType(), True),
    types.StructField('cool', types.IntegerType(), True),
    types.StructField('fans', types.IntegerType(), True),
    types.StructField('elite', types.StringType(), True),
    types.StructField('average_stars', types.FloatType(), True),
    types.StructField('compliment_hot', types.IntegerType(), True),
    types.StructField('compliment_more', types.IntegerType(), True),
    types.StructField('compliment_profile', types.IntegerType(), True),
    types.StructField('compliment_cute', types.IntegerType(), True),
    types.StructField('compliment_list', types.IntegerType(), True),
    types.StructField('compliment_note', types.IntegerType(), True),
    types.StructField('compliment_plain', types.IntegerType(), True),
    types.StructField('compliment_cool', types.IntegerType(), True),
    types.StructField('compliment_funny', types.IntegerType(), True),
    types.StructField('compliment_writer', types.IntegerType(), True),
    types.StructField('compliment_photos', types.IntegerType(), True),
    ])
    
NA_states = ["AB","BC","MB","NB","NL","NT","NS","NU","ON","PE","QC","SK","YT",
            "AL","AK","AS","AZ","AR","CA","CO","CT","DE","DC","FM","FL","GA",
            "GU","HI","ID","IL","IA","IN","KS","KY","LA","ME","MH","MD","MA",
            "MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND",
            "MP","OH","OK","OR","PW","PA","PR","RI","SC","SD","TN","TX","UT",
            "VT","VI","VA","WA","WV","WI","WY"]

def main(inputs, output):
    if "business" in inputs:
        yelp_business = spark.read.json(inputs)
        yelp_business = yelp_business.repartition(8)

        # Check Column Counts
        # yelp3 = yelp_business.select([functions.count(c) for c in yelp_business.columns])
        
        # Selecting businesses in North America
        # yelp_na = yelp_business.filter(yelp_business.state.isin(*NA_states) == True)
        
        # Selecting businesses which are open in North America
        # biz_open =  yelp_business.filter(yelp_business['is_open']==functions.lit(1))
        
        yelp_business.write.parquet(output+"/business", mode="overwrite")
    elif "review" in inputs:
        yelp_review = spark.read.json(inputs) 
        yelp_review = yelp_review.repartition(40)
        yelp_review.write.parquet(output+"/review", mode="overwrite")
    elif "user" in inputs:
        user_review = spark.read.json(inputs)
        user_review = user_review.repartition(40)
        user_review.write.parquet(output+"/user", mode="overwrite")
    else:
        print("\nCannot process the specified file\n")

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

