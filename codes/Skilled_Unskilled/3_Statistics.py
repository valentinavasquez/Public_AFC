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
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

''' LOAD DATA BASE (20% AFC DATA)'''
data = spark.read.csv(
    '/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/processed_data.csv', header=True, inferSchema=True)

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

#### 6. SHARE DE INGRESO POR WAGE_DATE Y AGREGADO POR CATEGORÍA DE SKILL

# Sumar ingreso total por Wage_Date
total_income_by_date = data.groupBy('Wage_Date').agg(F.sum('Taxable_Income').alias('Total_Income'))

# Sumar ingreso por Skill_Broad y Wage_Date
income_by_skill_broad_date = data.groupBy('Wage_Date', 'Skill_Broad').agg(F.sum('Taxable_Income').alias('Income_Skill'))
share_income_broad_date = income_by_skill_broad_date.join(total_income_by_date, on=['Wage_Date']) \
    .withColumn('Share_Income', F.round(F.col('Income_Skill') / F.col('Total_Income') * 100, 2)) \
    .orderBy('Wage_Date', 'Skill_Broad')

print("=== Share de ingreso por Wage_Date y Skill_Broad ===")
share_income_broad_date.show(20)

# Sumar ingreso por Skill_Strict y Wage_Date
income_by_skill_strict_date = data.groupBy('Wage_Date', 'Skill_Strict').agg(F.sum('Taxable_Income').alias('Income_Skill'))
share_income_strict_date = income_by_skill_strict_date.join(total_income_by_date, on=['Wage_Date']) \
    .withColumn('Share_Income', F.round(F.col('Income_Skill') / F.col('Total_Income') * 100, 2)) \
    .orderBy('Wage_Date', 'Skill_Strict')

print("=== Share de ingreso por Wage_Date y Skill_Strict ===")
share_income_strict_date.show(20)

# Exportar a CSV
share_income_broad_date.toPandas().to_csv(os.path.join(output_path, 'shares_income_skill_broad_by_date.csv'), index=False)
print("Exported: shares_income_skill_broad_by_date.csv")
print("Primeras filas de shares_income_skill_broad_by_date:")
share_income_broad_date.show(10)
share_income_strict_date.toPandas().to_csv(os.path.join(output_path, 'shares_income_skill_strict_by_date.csv'), index=False)
print("Exported: shares_income_skill_strict_by_date.csv")
print("Primeras filas de shares_income_skill_strict_by_date:")
share_income_strict_date.show(10)

# Estadística agregada (total)
total_income = data.agg(F.sum('Taxable_Income').alias('Total_Income')).collect()[0]['Total_Income']

# Skill_Broad agregado
income_by_skill_broad_total = data.groupBy('Skill_Broad').agg(F.sum('Taxable_Income').alias('Income_Skill'))
share_income_broad_total = income_by_skill_broad_total \
    .withColumn('Share_Income', F.round(F.col('Income_Skill') / F.lit(total_income) * 100, 2))

print("=== Share de ingreso agregado por Skill_Broad ===")
share_income_broad_total.show()
share_income_broad_total.toPandas().to_csv(os.path.join(output_path, 'shares_income_skill_broad_total.csv'), index=False)
print("Exported: shares_income_skill_broad_total.csv")
print("Primeras filas de shares_income_skill_broad_total:")
share_income_broad_total.show(10)

# Skill_Strict agregado
income_by_skill_strict_total = data.groupBy('Skill_Strict').agg(F.sum('Taxable_Income').alias('Income_Skill'))
share_income_strict_total = income_by_skill_strict_total \
    .withColumn('Share_Income', F.round(F.col('Income_Skill') / F.lit(total_income) * 100, 2))

print("=== Share de ingreso agregado por Skill_Strict ===")
share_income_strict_total.show()
share_income_strict_total.toPandas().to_csv(os.path.join(output_path, 'shares_income_skill_strict_total.csv'), index=False)
print("Exported: shares_income_skill_strict_total.csv")
print("Primeras filas de shares_income_skill_strict_total:")
share_income_strict_total.show(10)
