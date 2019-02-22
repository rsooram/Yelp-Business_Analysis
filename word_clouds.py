import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import functions, types
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("wordCloud").getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
from pyspark import SparkContext
spark.sparkContext.setLogLevel('WARN')
sc = SparkContext

#from wordcloud import WordCloud
import matplotlib.pyplot as pl
import re, operator, string
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram 
#from textblob import Word
#import nltk
#nltk.download('wordnet')

@functions.udf(returnType=types.StringType())
def clean(in_val):
    str_val = in_val.strip()
    table = str.maketrans({key: None for key in string.punctuation})
    clean_str = str_val.translate(table)
    return clean_str.upper() 

def main(bname, pcode):

	def words_ones(line):
		for y in line[0]:
			wordsep = re.compile(r'[%s]+' % re.escape(string.punctuation))
			k = wordsep.split(y)
			for w in k:
				if (len(w)>1):
					yield (w.upper(), 1)


	business_table = spark.read.parquet('YELP_PAR/business')
	review_table = spark.read.parquet('YELP_PAR/review')
	
	# Filtering reviews for a given bussiness into 3 stars and less as negative reviews and more than 3 stars as positive reviews
	if pcode == "all":
		id = business_table.filter(business_table.name == bname).select("business_id")
		nreviews = review_table.join(id,review_table.business_id == id.business_id).filter(review_table.stars <=3 ).select("text")
	else:
		id = business_table.filter(business_table.name == bname).filter(business_table.postal_code == pcode).collect()[0][0]
		nreviews = review_table.filter(review_table.business_id == id).filter(review_table.stars <= 3).select("text")

	# Tokenization of reviews
	tokenizer = Tokenizer(inputCol="text", outputCol="words")
	nwordsData = tokenizer.transform(nreviews)

	# Remove Stop-words and get word count
	remover = StopWordsRemover(inputCol="words", outputCol="filtered")
	nnostops = remover.transform(nwordsData).select("filtered").cache()
	ncleanwords = nnostops.rdd.flatMap(words_ones)
	
	# Summation of similar words to get frequency of the words
	nwordcount = ncleanwords.reduceByKey(operator.add)
	
	# Take top 30 frequent terms and save as csv file
	ntop30 = nwordcount.sortBy(lambda n: n[1], ascending = False).take(30)
	df = spark.createDataFrame(ntop30, ['word', 'count'])
	df.toPandas().to_csv(bname+"_uni.csv",index=False)

	# For bigrams
	ngram = NGram(n=2, inputCol="filtered", outputCol="ngrams")
	ngramDataFrame = ngram.transform(nnostops)
	bigrams = ngramDataFrame.select(ngramDataFrame["ngrams"])
	all_bigrams = bigrams.select(functions.explode("ngrams").alias("bigram"))
	clean_bigrams = all_bigrams.select(clean(all_bigrams["bigram"]).alias("clean_bigrams"))
	bigrams_length = clean_bigrams.withColumn('length', functions.size(functions.split(functions.col('clean_bigrams')," ")))
	req_bigrams = bigrams_length.filter(bigrams_length["length"]==2)
	bigram_count = req_bigrams.groupBy("clean_bigrams").count()
	bigram_top30 = bigram_count.orderBy("count", ascending = False).limit(30)
	bigram_top30.toPandas().to_csv(bname+"_bigrams.csv",index=False)
	
if __name__ == '__main__':
    name = sys.argv[1]
    code = sys.argv[2]
    main(name, code)
