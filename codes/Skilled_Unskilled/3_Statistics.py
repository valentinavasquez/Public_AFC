# This archive .py tiene como proposito crear la base de datos con ceros para la AFC, para luego agregar los ingresos por el seguro de cesantia
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
data = spark.read.csv(
    '/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/bases/processed_data.csv', header=True, inferSchema=True)

#### 1. COUNT OF WORKERS BY PERIOD AND SKILL LEVEL

# Count by Wage_Date and Skill_Broad
count_skill_broad = data.groupBy('Wage_Date', 'Skill_Broad') \
    .agg(F.countDistinct('ID_Person').alias('Count_Workers')) \
    .orderBy('Wage_Date', 'Skill_Broad')

print("=== Count by Period and Skill_Broad ===")
count_skill_broad.show(20)

# Count by Wage_Date and Skill_Strict
count_skill_strict = data.groupBy('Wage_Date', 'Skill_Strict') \
    .agg(F.countDistinct('ID_Person').alias('Count_Workers')) \
    .orderBy('Wage_Date', 'Skill_Strict')

print("=== Count by Period and Skill_Strict ===")
count_skill_strict.show(20)


#### 2. SHARES BY PERIOD (% Skilled vs Unskilled per month)

# Total workers per period
total_by_period = data.groupBy('Wage_Date') \
    .agg(F.countDistinct('ID_Person').alias('Total_Workers'))

# Shares for Skill_Broad
shares_broad = count_skill_broad.join(total_by_period, on=['Wage_Date']) \
    .withColumn('Share', F.round(F.col('Count_Workers') / F.col('Total_Workers') * 100, 2)) \
    .orderBy('Wage_Date', 'Skill_Broad')

print("=== Shares by Period - Skill_Broad (%) ===")
shares_broad.show(20)

# Shares for Skill_Strict
shares_strict = count_skill_strict.join(total_by_period, on=['Wage_Date']) \
    .withColumn('Share', F.round(F.col('Count_Workers') / F.col('Total_Workers') * 100, 2)) \
    .orderBy('Wage_Date', 'Skill_Strict')

print("=== Shares by Period - Skill_Strict (%) ===")
shares_strict.show(20)


#### 3. TOTAL SHARE OF SKILLED VS UNSKILLED (single number)

total_count = data.select(F.countDistinct('ID_Person')).collect()[0][0]

# Total share Skill_Broad
total_share_broad = data.groupBy('Skill_Broad') \
    .agg(F.countDistinct('ID_Person').alias('Count_Workers')) \
    .withColumn('Total_Share', F.round(F.col('Count_Workers') / F.lit(total_count) * 100, 2))

print("=== Total Share - Skill_Broad (%) ===")
total_share_broad.show()

# Total share Skill_Strict
total_share_strict = data.groupBy('Skill_Strict') \
    .agg(F.countDistinct('ID_Person').alias('Count_Workers')) \
    .withColumn('Total_Share', F.round(F.col('Count_Workers') / F.lit(total_count) * 100, 2))

print("=== Total Share - Skill_Strict (%) ===")
total_share_strict.show()


#### 4. AVERAGE WAGES BY PERIOD AND SKILL LEVEL

# Average Taxable_Income by period and Skill_Broad
avg_wage_broad = data.groupBy('Wage_Date', 'Skill_Broad') \
    .agg(F.round(F.avg('Taxable_Income'), 2).alias('Avg_Taxable_Income')) \
    .orderBy('Wage_Date', 'Skill_Broad')

print("=== Average Wages by Period - Skill_Broad ===")
avg_wage_broad.show(20)

# Average Taxable_Income by period and Skill_Strict
avg_wage_strict = data.groupBy('Wage_Date', 'Skill_Strict') \
    .agg(F.round(F.avg('Taxable_Income'), 2).alias('Avg_Taxable_Income')) \
    .orderBy('Wage_Date', 'Skill_Strict')

print("=== Average Wages by Period - Skill_Strict ===")
avg_wage_strict.show(20)


#### 5. PIVOT TO WIDE FORMAT AND EXPORT TO CSV

output_path = '/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/output'
os.makedirs(output_path, exist_ok=True)

# 1. Count by period - wide format
count_broad_wide = count_skill_broad.groupBy('Wage_Date').pivot('Skill_Broad').sum('Count_Workers').orderBy('Wage_Date')
count_strict_wide = count_skill_strict.groupBy('Wage_Date').pivot('Skill_Strict').sum('Count_Workers').orderBy('Wage_Date')

count_broad_wide.toPandas().to_csv(os.path.join(output_path, 'count_skill_broad.csv'), index=False)
print("Exported: count_skill_broad.csv")
count_strict_wide.toPandas().to_csv(os.path.join(output_path, 'count_skill_strict.csv'), index=False)
print("Exported: count_skill_strict.csv")

# 2. Shares by period - wide format
shares_broad_wide = shares_broad.groupBy('Wage_Date').pivot('Skill_Broad').sum('Share').orderBy('Wage_Date')
shares_strict_wide = shares_strict.groupBy('Wage_Date').pivot('Skill_Strict').sum('Share').orderBy('Wage_Date')

shares_broad_wide.toPandas().to_csv(os.path.join(output_path, 'shares_skill_broad.csv'), index=False)
print("Exported: shares_skill_broad.csv")
shares_strict_wide.toPandas().to_csv(os.path.join(output_path, 'shares_skill_strict.csv'), index=False)
print("Exported: shares_skill_strict.csv")

# 3. Total shares (already wide enough, just export)
total_share_broad.toPandas().to_csv(os.path.join(output_path, 'total_share_broad.csv'), index=False)
print("Exported: total_share_broad.csv")
total_share_strict.toPandas().to_csv(os.path.join(output_path, 'total_share_strict.csv'), index=False)
print("Exported: total_share_strict.csv")

# 4. Average wages by period - wide format
avg_wage_broad_wide = avg_wage_broad.groupBy('Wage_Date').pivot('Skill_Broad').sum('Avg_Taxable_Income').orderBy('Wage_Date')
avg_wage_strict_wide = avg_wage_strict.groupBy('Wage_Date').pivot('Skill_Strict').sum('Avg_Taxable_Income').orderBy('Wage_Date')

avg_wage_broad_wide.toPandas().to_csv(os.path.join(output_path, 'avg_wage_skill_broad.csv'), index=False)
print("Exported: avg_wage_skill_broad.csv")
avg_wage_strict_wide.toPandas().to_csv(os.path.join(output_path, 'avg_wage_skill_strict.csv'), index=False)
print("Exported: avg_wage_skill_strict.csv")

print(f"\nTodos los archivos exportados en: {output_path}")
