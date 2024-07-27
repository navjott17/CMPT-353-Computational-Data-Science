import sys
from pyspark.sql import SparkSession, functions, types, Row
import re
from pprint import pprint

spark = SparkSession.builder.appName('correlate logs').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred. Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        return Row(m[1], int(m[2]))
    else:
        return None


def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    log_lines = spark.sparkContext.textFile(in_directory)
    # TODO: return an RDD of Row() objects
    log_data = log_lines.map(line_to_row).filter(not_none)
    return log_data


def main(in_directory):
    logs = spark.createDataFrame(create_row_rdd(in_directory), schema='hostname:string, bytes:int')
    logs1 = logs.groupby('hostname').agg(functions.sum('bytes').alias('bytes'))
    logs2 = logs.groupby('hostname').count()
    logs_data = logs1.join(logs2, on='hostname')
    # reference from https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/
    logs_data = logs_data.select([functions.lit(1).alias('ones'), 'hostname', 'count', 'bytes'])
    logs_data = logs_data.withColumn('count_sqr', logs_data['count'] ** 2)
    logs_data = logs_data.withColumn('bytes_sqr', logs_data['bytes'] ** 2)
    logs_data = logs_data.withColumn('count_x_byte', logs_data['bytes'] * logs_data['count'])
    temp_logs1 = logs_data.groupby().sum()
    final_logs = temp_logs1.first()

    # TODO: calculate r.
    n = final_logs[0]
    sum_xi = final_logs[1]
    sum_yi = final_logs[2]
    sum_xi_sqr = final_logs[3]
    sum_yi_sqr = final_logs[4]
    sum_xi_yi = final_logs[5]

    num = (n*sum_xi_yi) - (sum_xi*sum_yi)
    denom = ((n*sum_xi_sqr)-(sum_xi**2))**(1/2) * ((n*sum_yi_sqr)-(sum_yi**2))**(1/2)

    # TODO: it isn't zero.
    r = num/denom
    print("r = %g\nr^2 = %g" % (r, r**2))


if __name__ == '__main__':
    in_directory = sys.argv[1]
    main(in_directory)
