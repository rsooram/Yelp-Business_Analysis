import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import pandas as pd
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import stddev, avg, broadcast, max
spark = SparkSession.builder.appName('Top_users').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.types import DateType

@functions.udf(returnType=types.StringType())
def get_monthyear_str(in_val):
    strlen = len(in_val)
    month_year = in_val[:strlen-3]
    # form = month_year[5:]+"-"+month_year[:-3]
    return month_year

@functions.udf(returnType=types.DateType())
def to_month_year(in_val):
    str_to_month = datetime.strptime(in_val, '%Y-%m')
    return str_to_month

def main(inputs, output):
    business_table = spark.read.parquet(inputs+'/business').cache()
    review_table = spark.read.parquet(inputs+'/review')
    business_groups = business_table.groupBy("name").sum("review_count")
    top_businesses = business_groups.orderBy("sum(review_count)", ascending=False).limit(1)
    biz_names_list = top_businesses.select("name").rdd.flatMap(lambda x: x).collect()
    outdata_pd = pd.DataFrame()
    for business in biz_names_list:
        biz_rows = business_table.filter(business_table.name == business)
        biz_ids = biz_rows.select(biz_rows["business_id"],biz_rows["name"])
        review_ids = review_table.select(review_table["review_id"],review_table["business_id"],review_table["stars"],review_table["date"])
        biz_cols = biz_ids.join(review_ids,["business_id"],"inner")
        biz_cols = biz_cols.select(biz_cols["name"],biz_cols["stars"],biz_cols["date"])
        biz_month_year = biz_cols.withColumn('month_year', get_monthyear_str(biz_cols["date"]))
        biz_month_year = biz_month_year.withColumn('month_year_date', to_month_year(biz_month_year["month_year"]))
        biz_month_year.createOrReplaceTempView('biz_month_year')
        stars_month = spark.sql("SELECT name,month_year_date, AVG(stars) AS avg_stars FROM biz_month_year GROUP BY name,month_year_date")
        data = stars_month.select(stars_month["name"],stars_month["month_year_date"],stars_month["avg_stars"])
        data_pd = data.toPandas()
        outdata_pd = outdata_pd.append(data_pd,ignore_index=True)
   outdata_pd.to_csv(output+".csv",index=False)
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

