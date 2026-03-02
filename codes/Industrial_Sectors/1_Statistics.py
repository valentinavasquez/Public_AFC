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

data.select(
    "Economic_Activity",
    "MIP_sector_id",
    "MIP_sector_name"
).show(30, truncate=False)