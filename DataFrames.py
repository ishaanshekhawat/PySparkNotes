from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# From a list of tuples (infer schema)
data = [(1, 'Alice', 50000.0, 'HR'), (2, 'Bob', 70000.0, 'ENG'), (3, 'Cara', 90000.0, 'ENG')]
df = spark.createDataFrame(data, ['id', 'name', 'salary', 'dept'])
df.show()
df.printSchema()

# Explicit schema
schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('name', StringType(), True),
    StructField('salary', DoubleType(), True),
    StructField('dept', StringType(), True)
])
df2 = spark.createDataFrame(data, schema)
df2.show()

# From a JSON/CSV/Parquet file (uncomment & set a real path)
csv_df = spark.read.option('header', True).option('inferSchema', True).csv('file:///path/to/data.csv')
json_df = spark.read.json('file:///path/to/data.json')
pq_df   = spark.read.parquet('file:///path/to/data.parquet')


from pyspark.sql.functions import col, lit, expr

df.select('name', 'salary').show()
df.select(col('name').alias('employee'), (col('salary') * 1.1).alias('salary_plus_10pct')).show()
df.selectExpr('name as employee', 'salary * 1.1 as salary_plus_10pct').show(5)

# Add / replace columns
df = df.withColumn('senior', (col('salary') >= 80000))
df.show()

# Literal column
df = df.withColumn('country', lit('IN'))
df.show()

# Drop columns
df.drop('country').show()

from pyspark.sql.functions import desc

df.filter(col('dept') == 'ENG').show()
df.where("salary >= 60000").orderBy(desc('salary')).show()
df.sort('dept', desc('salary')).limit(2).show()

from pyspark.sql.functions import avg, min as sf_min, max as sf_max, sum as sf_sum, count

df.groupBy('dept').agg(
    count('*').alias('n'),
    sf_min('salary').alias('min_salary'),
    avg('salary').alias('avg_salary'),
    sf_max('salary').alias('max_salary')
).show()

from pyspark.sql.functions import when

df = df.withColumn('band', when(col('salary') < 60000, 'L')
                           .when(col('salary') < 80000, 'M')
                           .otherwise('H'))
df.show()

dept_meta = spark.createDataFrame([
    ('HR', 'Human Resources'),
    ('ENG', 'Engineering')
], ['dept', 'dept_name'])

df.join(dept_meta, on='dept', how='inner').show()
df.join(dept_meta, on='dept', how='left').show()
df.join(dept_meta, on='dept', how='right').show()
df.join(dept_meta, on='dept', how='outer').show()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum as sf_sum

w = Window.partitionBy('dept').orderBy(desc('salary'))
df.select('dept', 'name', 'salary',
          row_number().over(w).alias('row_num'),
          rank().over(w).alias('rank'),
          dense_rank().over(w).alias('dense_rank'),
          lag('salary', 1).over(w).alias('prev_salary'),
          lead('salary', 1).over(w).alias('next_salary')
         ).show()

# Running total per department
w2 = Window.partitionBy('dept').orderBy('salary').rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.select('dept','name','salary', sf_sum('salary').over(w2).alias('running_total')).show()

na_df = spark.createDataFrame([
    (1, 'Al',  None),
    (2, None, 70000.0),
    (3, 'Cy',  90000.0)
], ['id','name','salary'])

na_df.show()
na_df.fillna({'name':'Unknown', 'salary':0}).show()
na_df.na.drop(subset=['name']).show()

from pyspark.sql.functions import udf, upper
from pyspark.sql.types import StringType

# Built-in function
df.select(upper(col('name')).alias('NAME'), 'dept').show()

# UDF example (only when necessary)
def reverse_str(s):
    return s[::-1] if s else s
reverse_udf = udf(reverse_str, StringType())
df.select('name', reverse_udf(col('name')).alias('reversed')).show()

# Temporary Views & Spark SQL
df.createOrReplaceTempView('employees')
spark.sql('SELECT dept, COUNT(*) AS n, AVG(salary) AS avg_salary FROM employees GROUP BY dept').show()

out_dir = 'output/df_demo_parquet'
# Write Parquet
df.write.mode('overwrite').parquet(out_dir)

# Read it back
back = spark.read.parquet(out_dir)
back.show()

# Other formats (uncomment & adjust)
df.write.mode('overwrite').json('output/df_demo_json')
df.write.mode('overwrite').option('header', True).csv('output/df_demo_csv')
