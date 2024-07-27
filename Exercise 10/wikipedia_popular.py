import sys
import re
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

pages_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.LongType()),
    types.StructField('bytes', types.LongType()),
])


def convert_to_str(date_hour):
    #reference from https://www.techbeamers.com/python-convert-list-string/#:~:text=Python%20provides%20a%20magical%20join%20%28%29%20method%20that,the%20join%20%28%29%20method%20in%20such%20different%20cases.
    result = ''.join(date_hour)
    return result


def date_time(filename):
    #reference from https://stackoverflow.com/questions/24895278/extract-substring-from-filename-in-python
    pattern = r'[0-9]{8}-[0-9]{2}'
    search = re.findall(pattern, filename)
    result = convert_to_str(search)
    return result


def main(in_directory, out_directory):
    pages = spark.read.csv(in_directory, sep=' ', schema=pages_schema).withColumn('filename', functions.input_file_name())
    pages = pages[pages['language'] == ('en')]
    pages = pages[pages['title'] != ('Main_Page')]
    pages = pages[~(pages['title'].startswith('Special:'))]
    path_to_hour = functions.udf(date_time, returnType=types.StringType())
    data = pages.select(
        pages['language'],
        pages['title'],
        pages['views'],
        pages['bytes'],
        pages['filename'],
        path_to_hour(pages['filename']).alias('date_hour')
    )
    new_data = data.groupby('date_hour').max('views')
    new_data.cache()
    new_data = new_data.withColumnRenamed('max(views)', 'views')
    df = new_data.join(data, ['date_hour', 'views'])
    df = df.sort('date_hour', 'title')
    df = df.drop('language', 'filename', 'bytes')
    df = df.select('date_hour', 'title', 'views')
    df.write.csv(out_directory, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)