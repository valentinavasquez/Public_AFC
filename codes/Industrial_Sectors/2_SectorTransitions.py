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
    '/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/processed_data.csv', header=True, inferSchema=True)

''' Create MIP sector classification from AFC economic activity '''
data = data.withColumn(
    "Economic_Activity_int",
    F.col("Economic_Activity").cast("int")
)
#Make sure that Taxable_Income is double for later calculations
data = data.withColumn("Taxable_Income", F.col("Taxable_Income").cast("double"))

# ----------------------------
# 2) Map economic activity -> MIP sector (1..12) + name
# ----------------------------
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

''' Calculate sector transitions '''

# Filter out rows without a valid MIP sector
data = data.filter(F.col("MIP_sector_id").isNotNull())

# Keep only one row per person per period: the one with the highest Taxable_Income
w_dedup = Window.partitionBy('ID_Person', 'Wage_Date').orderBy(F.col('Taxable_Income').desc())
persons = data.withColumn('_rank', F.row_number().over(w_dedup)) \
    .filter(F.col('_rank') == 1) \
    .select('ID_Person', 'Wage_Date', 'MIP_sector_id') \
    .drop('_rank')

# -----------------------------------------------------------------
# En vez de crossJoin, generar solo los meses entre primera y última
# aparición de cada persona y luego hacer left join con los datos reales
# -----------------------------------------------------------------

# Rango de fechas por persona
person_range = persons.groupBy("ID_Person").agg(
    F.min("Wage_Date").alias("min_date"),
    F.max("Wage_Date").alias("max_date")
)

# Generar secuencia mensual por persona (solo sus meses relevantes)
person_months = person_range.withColumn(
    "Wage_Date",
    F.explode(
        F.sequence(
            F.col("min_date").cast("date"),
            F.col("max_date").cast("date"),
            F.expr("interval 1 month")
        )
    )
).select("ID_Person", "Wage_Date")

# Left join con datos reales para obtener sector (null si no aparece)
full_panel = person_months.join(
    persons.select("ID_Person", "Wage_Date", F.col("MIP_sector_id").alias("Current_Sector")),
    on=["ID_Person", "Wage_Date"],
    how="left"
)

# Ventana por persona ordenada cronológicamente
w = Window.partitionBy("ID_Person").orderBy("Wage_Date")

# Sector del periodo anterior
# Marcar primer periodo de cada persona
full_panel = (full_panel
    .withColumn("Prev_Sector", F.lag("Current_Sector").over(w))
    .withColumn("is_first_period", (F.row_number().over(w) == 1))
)

# ----------------------------
# 6) Define STATE of each period (your event-state definition)
# ----------------------------
# State_t is a label for the period t:
# - New Worker (first observed month)
# - Unemployment (Current_Sector is null)
# - Entry (Prev null & Current not null)
# - SectorName otherwise

#full_panel = full_panel.withColumn(
#    "Transition_Type",
#    F.when(F.col("is_first_period"), F.lit("New Worker")) 
#     .when(F.col("Current_Sector").isNotNull() & F.col("Prev_Sector").isNull(), F.lit("Entry"))
#     .when(F.col("Current_Sector").isNull(), F.lit("Unemployment"))
#     .when(F.col("Current_Sector").isNotNull(), F.lit("Active"))
#     .otherwise(F.lit(None))
#)

full_panel = full_panel.withColumn(
    "State",
    F.when(F.col("is_first_period"), F.lit("New Worker"))
     .when(F.col("Current_Sector").isNull(), F.lit("Unemployment"))
     .when(F.col("Prev_Sector").isNull() & F.col("Current_Sector").isNotNull(), F.lit("Entry"))
     .otherwise(mip_name_expr[F.col("Current_Sector")])
)

# === Step 5: Matriz de transición y probabilidades ===
transitions = (full_panel
    .withColumn("State_From", F.lag("State").over(w))
    .withColumn("State_To", F.col("State"))
)

# Exclude transitions involving New Worker and missing State_From
transitions = transitions.filter(
    F.col("State_From").isNotNull() &
    (F.col("State_From") != F.lit("New Worker")) &
    (F.col("State_To") != F.lit("New Worker"))
)

# Compute P(State_To | State_From)
transition_counts = transitions.groupBy("State_From", "State_To").agg(F.count("*").alias("count"))

w_from = Window.partitionBy("State_From")
transition_probs = (transition_counts
    .withColumn("total_from", F.sum("count").over(w_from))
    .withColumn("probability", F.round(F.col("count") / F.col("total_from"), 6))
    .orderBy("State_From", "State_To")
)

print("\n=== Transition probabilities P(State_To | State_From) ===")
transition_probs.show(200, truncate=False)

# ----------------------------
#  Export (Spark write, no toPandas)
# ----------------------------
output_dir = "/Users/valentinavasquez/Documents/GitHub/Public_AFC/output/Industrial_Sectors"
os.makedirs(output_dir, exist_ok=True)

(transition_probs
 .coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(os.path.join(output_dir, "transition_probabilities_long"))
)

elapsed = time.time() - start_time
print(f"\nDone. Execution time: {elapsed:.2f} seconds")
print(f"Saved to: {output_dir}/transition_probabilities_long/")



# ----------------------------
# Create wide transition matrix
# ----------------------------
transition_probabilities_wide = (
    transition_probs
    .select("State_From", "State_To", "probability")
    .groupBy("State_From")
    .pivot("State_To")
    .agg(F.first("probability"))
    .fillna(0)
    .orderBy("State_From")
)

print("\n=== Transition probabilities (wide matrix) ===")
#transition_probabilities_wide.show(100, truncate=False)

# Exportar matriz de transición en formato wide

# Guardar matriz wide en CSV (usando transition_probabilities_wide)
output_path_wide = "/Users/valentinavasquez/Documents/GitHub/Public_AFC/output/Industrial_Sectors/transition_probabilities_wide.csv"
transition_probabilities_wide.toPandas().to_csv(output_path_wide, index=False)
print(f"Matriz de probabilidades de transición (wide) guardada en: {output_path_wide}")

# Guardar matriz long en CSV (sin crear carpeta)
output_path_long = "/Users/valentinavasquez/Documents/GitHub/Public_AFC/output/Industrial_Sectors/transition_probabilities_long.csv"
transition_probs.toPandas().to_csv(output_path_long, index=False)
print(f"Matriz de probabilidades de transición (long) guardada en: {output_path_long}")
