import sys
import string, re
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('word count').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+


def main(in_directory, out_directory):
    data = spark.read.text(in_directory)
    df = data.toDF('body')
    wordbreak = r'[%s\s]+' % (re.escape(string.punctuation),)
    df = df.select(functions.split(df['body'], wordbreak).alias('body'))
    df = df.select(functions.explode(df['body']).alias('body'))
    df = df.select(functions.lower(df['body']).alias('word'))
    df = df.filter(df['word'] != '')
    result = df.groupby('word').count()
    result = result.orderBy([result['count'], result['word']], ascending=[False, True])

    result.write.csv(out_directory, mode='overwrite')


if __name__ == '__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
