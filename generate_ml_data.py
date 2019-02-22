import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import stddev, avg, broadcast, max
from datetime import datetime
from pyspark.sql.functions import col, split
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql.functions import isnan, when, count, col
spark = SparkSession.builder.appName('ML_Data_Prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

@functions.udf(returnType=types.DateType())
def to_date(in_val):
    str_to_date = datetime.strptime(in_val, '%Y-%m-%d')
    return str_to_date

@functions.udf(returnType=types.IntegerType())
def num_months(in_val):
    cur_date = datetime.strptime("2018-11-01", '%Y-%m-%d')
    months = (cur_date.year - in_val.year)*12 + (cur_date.month - in_val.month)
    return months

@functions.udf(returnType=types.StringType())
def useful_cat(in_val):
    if in_val<=3:
        return "not_useful"
    elif in_val>= 4 and in_val<= 7:
        return "slightly_useful"
    elif in_val>= 8 and in_val<= 10 :
        return "moderatly_useful"
    elif in_val>= 11 and in_val<= 16:
        return "highly_useful"
    else:
        return "extremely_useful"

def main(inputs):
    user_table = spark.read.parquet(inputs+'/user')
    review_table = spark.read.parquet(inputs+'/review')
    review_table = review_table.select(review_table["user_id"],review_table["text"],review_table["useful"])\
                            .withColumnRenamed("useful","review_useful_votes")
    merge_table = user_table.join(review_table,["user_id"],"inner")
    merge_table = merge_table.drop("name","elite","funny","cool")
    merge_table = merge_table.withColumn('friends_count', functions.size(functions.split(functions.col('friends'), ",")))
    merge_table = merge_table.withColumn('reviewtext_count', functions.size(functions.split(functions.col('text'), " ")))
    merge_table = merge_table.withColumn('yelp_date', to_date(merge_table["yelping_since"]))
    merge_table = merge_table.withColumn('no_months', num_months(merge_table["yelp_date"]))
    merge_table = merge_table.withColumn('useful_category', useful_cat(merge_table["review_useful_votes"]))
    merge_table = merge_table.drop("yelp_date","yelping_since","friends","text","user_id")
    train, test = merge_table.randomSplit([0.80, 0.20],seed=456)
    train = train.repartition(8)
    train.write.parquet("MLearn/train", mode='overwrite')
    test = test.repartition(8)
    test.write.parquet("MLearn/test", mode='overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
