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
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

''' LOAD DATA BASE (20% AFC DATA)'''
data = spark.read.csv(
    '/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/bases/Data_20_percent.csv', header=True, inferSchema=True)



# Filter out rows with empty or null Birthdate/Wage_Date before processing
data = data.filter(F.col('Birthdate').isNotNull() & (F.col('Birthdate').cast('string') != ''))
data = data.filter(F.col('Wage_Date').isNotNull() & (F.col('Wage_Date').cast('string') != ''))

# Commenting out the remaining lines to ensure only data.show(5) runs
data = data.withColumn('Birthdate', F.col('Birthdate').cast('string'))
data = data.withColumn('Birth_Year', F.substring(
     'Birthdate', 1, 4).cast(IntegerType()))
data = data.withColumn('Birth_Month', F.substring(
     'Birthdate', 5, 6).cast(IntegerType()))
data = data.withColumn('Birthdate', F.to_date(
     F.concat_ws('-', 'Birth_Year', 'Birth_Month', F.lit('01'))))

# Process Wage_Date to extract year, month, and create a proper date column
data = data.withColumn('Wage_Date', F.col('Wage_Date').cast('string'))
data = data.withColumn('Wage_Year', F.substring('Wage_Date', 1, 4).cast(IntegerType()))
data = data.withColumn('Wage_Month', F.substring('Wage_Date', 5, 6).cast(IntegerType()))
data = data.withColumn('Wage_Date', F.to_date(F.concat_ws('-', 'Wage_Year', 'Wage_Month', F.lit('01'))))

###### CREATE AGE VARIABLE
data = data.withColumn('Age', (F.year('Wage_Date') - F.year('Birthdate') -
                                F.when(F.month('Wage_Date') < F.month('Birthdate'), 1).otherwise(0)))
##### FILTER DATA TO THE PERIOD 2006-2026 AND AGE 15-60
data = data.filter((F.col('Wage_Year') >= 2006) & (F.col('Wage_Date') <= F.lit('2026-02-28')))
data = data.filter((F.col('Age') >= 15) & (F.col('Age') <= 60))


####CREATE EDUCATION LEVEL VARIABLES
# skill_broad: college uncompleted is considered Skilled
skill_broad = {
    0: "Unskilled", 1: "Unskilled", 2: "Unskilled",
    3: "Unskilled", 4: "Unskilled", 5: "Unskilled",
    6: "Unskilled", 7: "Unskilled", 8: "Unskilled",
    9: "Unskilled", 10: "Skilled", 11: "Skilled",
    12: "Skilled", 13: "Skilled", 14: "Skilled",
    15: "Skilled", 16: "Skilled", 17: "Skilled",
    99: "No Information"
}

# skill_strict: college uncompleted is considered Unskilled
skill_strict = {
    0: "Unskilled", 1: "Unskilled", 2: "Unskilled",
    3: "Unskilled", 4: "Unskilled", 5: "Unskilled",
    6: "Unskilled", 7: "Unskilled", 8: "Unskilled",
    9: "Unskilled", 10: "Unskilled", 11: "Skilled",
    12: "Skilled", 13: "Skilled", 14: "Skilled",
    15: "Skilled", 16: "Skilled", 17: "Skilled",
    99: "No Information"
}

# Map Educ_Level using skill_broad: college uncompleted is considered Skilled
exprs_broad = [F.when(F.col('Educ_Level') == k, v) for k, v in skill_broad.items()]
data = data.withColumn('Skill_Broad', F.coalesce(*exprs_broad))

# Map Educ_Level using skill_strict: college uncompleted is considered Unskilled
exprs_strict = [F.when(F.col('Educ_Level') == k, v) for k, v in skill_strict.items()]
data = data.withColumn('Skill_Strict', F.coalesce(*exprs_strict))



# Filter out rows with 'No Information' in the Skill_Broad and Skill_Strict columns
data = data.filter(F.col('Skill_Broad') != 'No Information')
data = data.filter(F.col('Skill_Strict') != 'No Information')

# Export to a single CSV file in the 'bases' folder
import glob, shutil
output_dir = '/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/bases/processed_data_tmp'
final_path = '/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/bases/processed_data.csv'

# Remove old folder/file if it exists
if os.path.isdir(final_path):
    shutil.rmtree(final_path)
elif os.path.isfile(final_path):
    os.remove(final_path)

data.write.csv(output_dir, header=True, mode='overwrite')

# Merge part files into a single CSV
part_files = sorted(glob.glob(os.path.join(output_dir, 'part-*.csv')))
with open(final_path, 'w') as outfile:
    for i, f in enumerate(part_files):
        with open(f, 'r') as infile:
            if i == 0:
                outfile.write(infile.read())
            else:
                next(infile)  # skip header
                outfile.write(infile.read())
shutil.rmtree(output_dir)
print(f"CSV exportado exitosamente a: {final_path}")

# Display the specified columns including Skill_Level_H and Skill_Level_L
data.select('ID_Person', 'Birthdate', 'Educ_Level', 'Wage_Date', 'Taxable_Income', 'Skill_Strict', 'Skill_Broad').show(5)








