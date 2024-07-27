import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('weather ETL').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.4' # make sure we have Spark 2.4+

observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])


def main(in_directory, out_directory):

    weather = spark.read.csv(in_directory, schema=observation_schema)
    cleaned_data = weather
    cleaned_data = cleaned_data.filter(cleaned_data.qflag.isNull())
    cleaned_data = cleaned_data.filter(cleaned_data.station.startswith('CA'))
    cleaned_data = cleaned_data.filter(cleaned_data['observation'] == 'TMAX')

    #referenced from https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/
    cleaned_data = cleaned_data.withColumn('tmax', col('value')/10)

    #referenced from https://stackoverflow.com/questions/51689460/select-specific-columns-from-spark-dataframe
    cleaned_data = cleaned_data.select(cleaned_data['station'], cleaned_data['date'], cleaned_data['tmax'])

    # TODO: finish here.

    cleaned_data.write.json(out_directory, compression='gzip', mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
