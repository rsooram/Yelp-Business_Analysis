import sys
import pandas as pd
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import stddev, avg, broadcast, max
spark = SparkSession.builder.appName('Top_users').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

def normalize_max(df, columns):
     aggExpr = []
     for column in columns:
        aggExpr.append(max(df[column]).alias(column +'_max'))
     maxvalue = df.agg(*aggExpr).collect()[0]
     for column in columns:            
        df = df.withColumn(column+'_norm',(df[column]/maxvalue[column+'_max']))  
     return df

def main(inputs, output):
    business_table = spark.read.parquet(inputs+'/business').cache()
    user_table = spark.read.parquet(inputs+'/user')
    review_table = spark.read.parquet(inputs+'/review')
    user_avg_votes = review_table.groupBy("user_id").avg("useful").cache()
    review_ids = review_table.select(review_table["review_id"],review_table["user_id"],review_table["business_id"]).cache()
    #user_name = user_table.select(user_table["user_id"],user_table["name"])
    user_with_avg_votes = user_table.join(user_avg_votes,["user_id"],"inner")
    business_groups = business_table.groupBy("name").sum("review_count")
    top_businesses = business_groups.orderBy("sum(review_count)", ascending=False).limit(10)
    # top_businesses names list 
    biz_names_list = top_businesses.select("name").rdd.flatMap(lambda x: x).collect()
    #outdata = spark.createDataFrame([],schema[])
    outdata_pd = pd.DataFrame()
    for business in biz_names_list:
        biz_table = business_table.filter(business_table["name"] == functions.lit(business))
        biz_ids = biz_table.select(biz_table["business_id"],biz_table["name"].alias("business_name"))
        all_users = review_ids.join(broadcast(biz_ids),["business_id"],"inner").join(user_with_avg_votes,["user_id"],"inner")
        feature_cols = all_users.select(all_users["user_id"],all_users["business_name"],all_users["name"],all_users["fans"],all_users["review_count"],all_users["avg(useful)"]).dropDuplicates()
        cols = ['fans', 'review_count', 'avg(useful)']
        cols_norm = normalize_max(feature_cols, cols)
        user_score = cols_norm.withColumn("score",cols_norm['fans_norm']+cols_norm['review_count_norm']+cols_norm['avg(useful)_norm'])\
                .select("user_id","business_name","name","score","fans_norm","review_count_norm","avg(useful)_norm")
        data = user_score.orderBy("score", ascending=False).limit(10)
        data_pd = data.toPandas()
        outdata_pd = outdata_pd.append(data_pd,ignore_index=True)
    outdata_pd.to_csv(output+".csv",index=False)
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
