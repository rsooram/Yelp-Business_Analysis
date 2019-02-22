import sys
import pandas as pd
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import stddev, avg, broadcast, max
spark = SparkSession.builder.appName('top_businesses').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


def main(inputs, output):
    business_table = spark.read.parquet(inputs+'/business')
    business_groups = business_table.groupBy("name").sum("review_count")
    top_businesses = business_groups.orderBy("sum(review_count)", ascending=False).limit(10)
    biz_names_list = top_businesses.select("name").rdd.flatMap(lambda x: x).collect()
    top_10_biz = business_table.filter(business_table.name.isin(*biz_names_list) == True)
    top_10_biz.createOrReplaceTempView('top_10_biz')
    top_reviewed_businesses = spark.sql("SELECT name as business_name, SUM(review_count) as review_count, AVG(stars) as stars FROM top_10_biz GROUP BY name")
    data_pd = top_reviewed_businesses.toPandas()
    data_pd.to_csv(output+".csv",index=False)
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
