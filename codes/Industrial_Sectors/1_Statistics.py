import os
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date, expr, when, lit, last
from scipy.stats import kurtosis
from scipy.stats import skew
import time
start_time = time.time()

# Reiniciar la sesión de Spark con la nueva configuración
spark = SparkSession.builder.master('local[*]') \
    .appName("Optimización con PySpark") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()

''' LOAD DATA BASE (20% AFC DATA)'''
#This database is created with the 1_MergeData.py and 2_DataCleaning.py files, which are in the Skilled_Unskilled folder.
data = spark.read.csv(
    '/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/bases/processed_data.csv', header=True, inferSchema=True)

''' Create MIP sector classification from AFC economic activity '''
data = data.withColumn(
    "Economic_Activity_int",
    F.col("Economic_Activity").cast("int")
)

mip_sector_id_map = {
    1: 1,   # Agriculture, forestry and fishing
    2: 2,   # Mining
    3: 3,   # Manufacturing
    4: 4,   # Electricity, gas, steam and air conditioning
    5: 4,   # Water supply, waste management
    6: 5,   # Construction
    7: 6,   # Wholesale and retail trade
    8: 7,   # Transport and storage
    9: 6,   # Accommodation and food services
    10: 7,  # Information and communications
    11: 8,  # Financial and insurance activities
    12: 9,  # Real estate activities
    13: 10, # Professional, scientific and technical activities
    14: 10, # Administrative and support services
    15: 12, # Public administration and defense
    16: 11, # Education
    17: 11, # Human health and social work
    18: 11, # Arts, entertainment and recreation
    19: 11, # Other service activities
    20: 12, # Extraterritorial organizations
    21: None  # Missing / no information
}

mip_sector_name_map = {
    1: "Agriculture, forestry and fishing",
    2: "Mining",
    3: "Manufacturing",
    4: "Electricity, gas, water and waste management",
    5: "Construction",
    6: "Wholesale and retail trade; accommodation and food services",
    7: "Transport, communications and information services",
    8: "Financial intermediation",
    9: "Real estate and housing services",
    10: "Business services",
    11: "Personal services",
    12: "Public administration"
}


mip_id_expr = F.create_map([F.lit(x) for kv in mip_sector_id_map.items() for x in kv])
mip_name_expr = F.create_map([F.lit(x) for kv in mip_sector_name_map.items() for x in kv])

data = (
    data
    .withColumn("MIP_sector_id", mip_id_expr[F.col("Economic_Activity_int")])
    .withColumn(
        "MIP_sector_name",
        F.when(F.col("MIP_sector_id").isNull(), F.lit(None))
         .otherwise(mip_name_expr[F.col("MIP_sector_id")])
    )
    .drop("Economic_Activity_int")
)

''' Calculate statistics by MIP sector '''

# Filter out rows without a valid MIP sector
data = data.filter(F.col("MIP_sector_id").isNotNull())

#### 1. COUNT OF WORKERS BY PERIOD AND MIP SECTOR

count_by_sector = data.groupBy('Wage_Date', 'MIP_sector_id', 'MIP_sector_name') \
    .agg(F.countDistinct('ID_Person').alias('Count_Workers')) \
    .orderBy('Wage_Date', 'MIP_sector_id')

print("=== Count of Workers by Period and MIP Sector ===")
count_by_sector.show(20, truncate=False)


#### 2. SHARES OF WORKERS BY PERIOD AND MIP SECTOR (%)

# Total workers per period
total_by_period = data.groupBy('Wage_Date') \
    .agg(F.countDistinct('ID_Person').alias('Total_Workers'))

# Calculate shares
shares_by_sector = count_by_sector.join(total_by_period, on=['Wage_Date']) \
    .withColumn('Share', F.round(F.col('Count_Workers') / F.col('Total_Workers') * 100, 2)) \
    .orderBy('Wage_Date', 'MIP_sector_id')

print("=== Shares of Workers by Period and MIP Sector (%) ===")
shares_by_sector.show(20, truncate=False)


#### 3. AVERAGE WAGE BY PERIOD AND MIP SECTOR

avg_wage_by_sector = data.groupBy('Wage_Date', 'MIP_sector_id', 'MIP_sector_name') \
    .agg(F.round(F.avg('Taxable_Income'), 2).alias('Avg_Taxable_Income')) \
    .orderBy('Wage_Date', 'MIP_sector_id')

print("=== Average Wage by Period and MIP Sector ===")
avg_wage_by_sector.show(20, truncate=False)


#### 4. AGGREGATE STATISTICS BY MIP SECTOR (across all periods)

# Total count of workers by sector (aggregate)
total_count_all = data.select(F.countDistinct('ID_Person')).collect()[0][0]

agg_count_by_sector = data.groupBy('MIP_sector_id', 'MIP_sector_name') \
    .agg(F.countDistinct('ID_Person').alias('Count_Workers')) \
    .orderBy('MIP_sector_id')

# Share of workers by sector (aggregate)
agg_shares_by_sector = agg_count_by_sector \
    .withColumn('Share', F.round(F.col('Count_Workers') / F.lit(total_count_all) * 100, 2)) \
    .orderBy('MIP_sector_id')

print("=== Aggregate Count and Share of Workers by MIP Sector ===")
agg_shares_by_sector.show(20, truncate=False)

# Average wage by sector (aggregate)
agg_avg_wage_by_sector = data.groupBy('MIP_sector_id', 'MIP_sector_name') \
    .agg(F.round(F.avg('Taxable_Income'), 2).alias('Avg_Taxable_Income')) \
    .orderBy('MIP_sector_id')

print("=== Aggregate Average Wage by MIP Sector ===")
agg_avg_wage_by_sector.show(20, truncate=False)


#### 5. PIVOT TO WIDE FORMAT AND EXPORT TO CSV

output_path = '/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/output/Industrial_Sectors'
os.makedirs(output_path, exist_ok=True)

# Count by period - wide format (columns = sector names)
count_wide = count_by_sector.groupBy('Wage_Date').pivot('MIP_sector_name').sum('Count_Workers').orderBy('Wage_Date')
count_wide.toPandas().to_csv(os.path.join(output_path, 'count_workers_by_sector.csv'), index=False)
print("Exported: count_workers_by_sector.csv")

# Shares by period - wide format (columns = sector names)
shares_wide = shares_by_sector.groupBy('Wage_Date').pivot('MIP_sector_name').sum('Share').orderBy('Wage_Date')
shares_wide.toPandas().to_csv(os.path.join(output_path, 'shares_workers_by_sector.csv'), index=False)
print("Exported: shares_workers_by_sector.csv")

# Average wage by period - wide format (columns = sector names)
avg_wage_wide = avg_wage_by_sector.groupBy('Wage_Date').pivot('MIP_sector_name').sum('Avg_Taxable_Income').orderBy('Wage_Date')
avg_wage_wide.toPandas().to_csv(os.path.join(output_path, 'avg_wage_by_sector.csv'), index=False)
print("Exported: avg_wage_by_sector.csv")

# Aggregate count and shares by sector
agg_shares_by_sector.toPandas().to_csv(os.path.join(output_path, 'agg_count_shares_by_sector.csv'), index=False)
print("Exported: agg_count_shares_by_sector.csv")

# Aggregate average wage by sector
agg_avg_wage_by_sector.toPandas().to_csv(os.path.join(output_path, 'agg_avg_wage_by_sector.csv'), index=False)
print("Exported: agg_avg_wage_by_sector.csv")

print(f"\nAll files exported to: {output_path}")

elapsed = time.time() - start_time
print(f"Execution time: {elapsed:.2f} seconds")
