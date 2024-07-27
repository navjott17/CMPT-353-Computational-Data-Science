import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit relative scores').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    #types.StructField('year', types.IntegerType()),
    #types.StructField('month', types.IntegerType()),
])


def main(in_directory, out_directory):
    comments = spark.read.json(in_directory, schema=comments_schema)

    # TODO
    data = comments.select('subreddit', 'score', 'author')
    data = data.cache()

    # reference from https://sparkbyexamples.com/spark/using-groupby-on-dataframe/
    averages_by_score = data.groupby('subreddit').avg('score')
    averages_by_score = averages_by_score.withColumnRenamed('avg(score)', 'average_score')
    averages_by_score = averages_by_score.filter(averages_by_score['average_score'] > 0)

    new_data = data.join(averages_by_score.hint('broadcast'), on='subreddit')
    new_data = new_data.withColumn('rel_score', new_data['score']/new_data['average_score'])
    new_data = new_data.cache()
    subreddit_data = new_data.groupby('subreddit').max('rel_score')
    author_data = new_data.groupby('subreddit', 'author').max('rel_score')

    subreddit_data = subreddit_data.withColumnRenamed('max(rel_score)', 'rel_score')
    author_data = author_data.withColumnRenamed('max(rel_score)', 'rel_score')

    best_author = author_data.join(subreddit_data.hint('broadcast'), on='rel_score').drop(author_data.subreddit)
    best_author = best_author.select('subreddit', 'author', 'rel_score')

    best_author.write.json(out_directory, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
