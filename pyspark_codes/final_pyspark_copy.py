import argparse
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.feature import StopWordsRemover, Tokenizer, RegexTokenizer
from pyspark.sql.functions import concat_ws,expr,col,array_contains,format_number,regexp_replace,lit
from pyspark.sql.types import DoubleType,LongType


###timestamp argparse
parser = argparse.ArgumentParser()
parser.add_argument("--date", type=str, help="airflow timestamp", default="2021-11-26")
args = parser.parse_args()
time = args.date

spark = SparkSession.builder.appName('Final_code').getOrCreate()

### df_movies cleaning
df_movies = spark.read.csv('s3://data-bootcamp-jose/Data/movie_review.csv',header=True)

tokenizer = Tokenizer(outputCol="words_old")
tokenizer.setInputCol("review_str")
df_movies = tokenizer.transform(df_movies)

remover = StopWordsRemover()
remover.setInputCol("words_old")
remover.setOutputCol("words")

df_movies = remover.transform(df_movies)
df_movies = df_movies.drop('words_old')
df_movies = df_movies.withColumn('positive', array_contains(df_movies.words, 'good'))
df_movies = df_movies.withColumn("positive_review", expr("CASE WHEN positive = 'true' THEN 1 " +
               "ELSE 0 END"))
df_movies = df_movies.drop('review_str','words','positive')

df_movies = df_movies.groupby('cid').agg(f.count('positive_review').alias('review_count'),f.sum('positive_review').alias('review_score'))

### df_user cleaning

df_user = spark.read.csv('s3://data-bootcamp-jose/Data/user_purchase.csv',header=True)

df_user = df_user.withColumn("Quantity", regexp_replace(df_user.Quantity, r"\W", ""))
df_user = df_user.na.fill(0)
df_user = df_user.withColumn("Quantity",df_user.Quantity.cast(LongType()))
df_user = df_user.withColumn("UnitPrice",df_user.UnitPrice.cast(LongType()))
df_user = df_user.withColumn('amount_spent', df_user['Quantity'] * df_user['UnitPrice'])

df_user = df_user.groupBy("CustomerID").agg(f.sum(df_user.amount_spent).alias('sum'))
df_user = df_user.withColumn("amount_spent", format_number(df_user.sum, 2)).drop('sum')
df_user = df_user.withColumn("amount_spent", regexp_replace(df_user.amount_spent, ",", ""))
df_user = df_user.withColumn("amount_spent",df_user.amount_spent.cast(LongType()))

df_joined = df_user.join(df_movies,df_movies.cid == df_user.CustomerID).drop('cid')
df_joined = df_joined.withColumn("CustomerID",df_joined.CustomerID.cast(LongType()))

### Add date
### x should be the timestamp
x = time
df_joined = df_joined.withColumn("insert_date",lit(x))

df_joined.repartition(1).write.format('csv').option('header','true').save('s3a://data-bootcamp-jose/final_result',mode='overwrite')