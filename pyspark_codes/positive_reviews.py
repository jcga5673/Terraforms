import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.feature import StopWordsRemover, Tokenizer, RegexTokenizer
from pyspark.sql.functions import concat_ws,expr,col,array_contains


spark = SparkSession.builder.appName('Third-example').getOrCreate()
#df = spark.read.csv('movie_review.csv',header=True)
df = spark.read.csv('"s3://data-bootcamp-jose/movie_review.csv',header=True)
###Tokenizer
tokenizer = Tokenizer(outputCol="words_old")
tokenizer.setInputCol("review_str")
df = tokenizer.transform(df)
#df1.show(1)
### Stop remover
remover = StopWordsRemover()
remover.setInputCol("words_old")
remover.setOutputCol("words")
df = remover.transform(df)
df = df.drop('words_old')
#df1.show(1)
df_sol = df.withColumn('positive', array_contains(df.words, 'good'))
df_sol = df_sol.withColumn("positive_review", expr("CASE WHEN positive = 'true' THEN 1 " +
               "ELSE 0 END"))
df_sol = df_sol.drop('review_str','words','positive')
df_sol.show()