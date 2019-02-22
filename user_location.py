import sys
import pandas as pd
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import stddev, avg, broadcast, max
spark = SparkSession.builder.appName('Top_users').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

# CAN - "AB","BC","MB","NB","NL","NT","NS","NU","ON","PE","QC","SK","YT",

NA_states = ["AL","AK","AS","AZ","AR","CA","CO","CT","DE","DC","FM","FL","GA",
            "GU","HI","ID","IL","IA","IN","KS","KY","LA","ME","MH","MD","MA",
            "MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND",
            "MP","OH","OK","OR","PW","PA","PR","RI","SC","SD","TN","TX","UT",
            "VT","VI","VA","WA","WV","WI","WY"]

def main(inputs, output):
    business_table = spark.read.parquet(inputs+'/business')
    user_table = spark.read.parquet(inputs+'/user')
    review_table = spark.read.parquet(inputs+'/review')
    business_groups = business_table.groupBy("name").sum("review_count")
    top_businesses = business_groups.orderBy("sum(review_count)", ascending=False).limit(10)
    biz_names_list = top_businesses.select("name").rdd.flatMap(lambda x: x).collect()
    business_table = business_table.withColumnRenamed("name","b_name")
    business_table = business_table.drop("review_count")
    business_table = business_table.filter(business_table.state.isin(*NA_states) == True).cache()
    review_ids = review_table.select(review_table["review_id"],review_table["user_id"],review_table["business_id"]).cache()
    user_table = user_table.select(user_table["user_id"],user_table["review_count"].alias("user_review_count"))
    outdata_pd = pd.DataFrame()
    for business_name in biz_names_list:
        biz_table = business_table.filter(business_table["b_name"] == functions.lit(business))
        all_users = review_ids.join(broadcast(biz_table),["business_id"],"inner").join(user_table,["user_id"],"inner").dropDuplicates()
        state_review_count = all_users.groupBy("b_name","state","city").sum("user_review_count")
        data_pd = state_review_count.toPandas()
        outdata_pd = outdata_pd.append(data_pd,ignore_index=True)
    outdata_pd.to_csv(output+".csv",index=False)
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
