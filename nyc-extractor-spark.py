# Data Pipeline with spark
# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sodapy import Socrata

#Get session
spark = SparkSession.builder.getOrCreate()

#Fetch data from NYC Data
client=Socrata("data.cityofnewyork.us","<APP_TOKEN>",username="<EMAIL>",password="<PASSWORD>")
results=client.get("43nn-pn8j",limit=1000000)
print('Data Fetched')

# Create dataframe
df=spark.createDataFrame(results)
# Rename some columns to identify attributes
df=df.withColumnRenamed('camis','id').withColumnRenamed('dba','name').withColumnRenamed('building','building_number').withColumnRenamed('zipcode','zip_code')

# Select only some columns, not at all
df=df[[
    'id',
    'name',
    'boro',
    'building_number',
    'street',
    'zip_code',
    'phone',
    'cuisine_description',
    'latitude',
    'longitude',
    'community_board',
    'council_district',
    'census_tract',
    'bin',
    'bbl',
    'nta'
]]

# Read table from Hive metastore
historial=spark.read.table('restaurant')

# Merge historical and current dataframes
merged=df.union(historial)

# Drop duplicates
merged=merged.dropDuplicates(['id'])
print('Clean Data')

# Overwrite entire table to prevent duplicates
merged.write.mode('overwrite').saveAsTable('restaurant')
print('Data Saved')