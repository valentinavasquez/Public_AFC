# =========================================================
# AKM PREPROCESSING - PYSPARK
# Valentina - AFC data
# =========================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# =========================================================
# 1. START SPARK
# =========================================================

spark = SparkSession.builder.master('local[*]') \
    .appName("Optimización con PySpark") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "12g") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

# =========================================================
# 2. LOAD MAIN DATA
# =========================================================

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/processed_data.csv")
)

# =========================================================
# 3. DEFINE CORE VARIABLES
# =========================================================

df = (
    df
    .withColumn("wid", F.col("id_person").cast("long"))
    .withColumn("fid", F.col("id_employer").cast("long"))
    .withColumn("year", F.col("wage_year").cast("int"))
    .withColumn("month", F.col("wage_month").cast("int"))
    .withColumn("wage", F.col("taxable_income").cast("double"))
    .withColumn("age", F.col("age").cast("double"))
)

# filtros básicos
df = df.filter(
    F.col("wid").isNotNull() &
    F.col("fid").isNotNull() &
    F.col("year").isNotNull() &
    F.col("month").isNotNull() &
    F.col("wage").isNotNull() &
    (F.col("wage") > 0)
)

# =========================================================
# 4. LOAD IPC
# =========================================================

ipc = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/ipc_clean.csv")
)

ipc = (
    ipc
    .select(
        F.col("year").cast("int"),
        F.col("month").cast("int"),
        F.col("ipc_def").cast("double")
    )
)

# =========================================================
# 5. MERGE IPC + DEFLATE
# =========================================================

df = df.join(ipc, on=["year", "month"], how="inner")

df = (
    df
    .withColumn("wage_real", F.col("wage") / F.col("ipc_def") * 100)
    .filter(F.col("wage_real") > 0)
    .withColumn("ln_wage", F.log(F.col("wage_real")))
)

# =========================================================
# 6. KEEP ONE JOB PER PERSON-MONTH
# =========================================================

w_main = Window.partitionBy("wid", "year", "month").orderBy(
    F.col("wage_real").desc(),
    F.col("fid").desc()
)

df = (
    df
    .withColumn("rn", F.row_number().over(w_main))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# =========================================================
# 7. AGE CONTROLS
# =========================================================

df = df.filter(F.col("age").isNotNull())

df = (
    df
    .withColumn("edad_norm", (F.col("age") - 40) / 40)
    .withColumn("edad_norm2", F.col("edad_norm") ** 2)
    .withColumn("edad_norm3", F.col("edad_norm") ** 3)
)

# =========================================================
# 8. TENURE (ROBUST VERSION)
# =========================================================

# convertir fecha a índice continuo
df = df.withColumn("date_index", F.col("year") * 12 + F.col("month"))

w_tenure = Window.partitionBy("wid", "fid").orderBy("year", "month")

df = (
    df
    .withColumn("lag_date", F.lag("date_index").over(w_tenure))
    .withColumn(
        "new_spell",
        F.when(F.col("lag_date").isNull(), 1)
         .when(F.col("date_index") - F.col("lag_date") != 1, 1)
         .otherwise(0)
    )
)

# identificar spells
w_spell = Window.partitionBy("wid", "fid").orderBy("year", "month") \
                .rowsBetween(Window.unboundedPreceding, 0)

df = df.withColumn("spell_id", F.sum("new_spell").over(w_spell))

# tenure dentro de cada spell
w_spell_order = Window.partitionBy("wid", "fid", "spell_id").orderBy("year", "month")

df = df.withColumn("tenure", F.row_number().over(w_spell_order))

# =========================================================
# 9. FINAL DATASET
# =========================================================

df_final = df.select(
    "wid", "fid", "year", "month",
    "wage_real", "ln_wage",
    "age", "edad_norm", "edad_norm2", "edad_norm3",
    "tenure",
    "gender", "educ_level", "skill_broad", "skill_strict"
)

# =========================================================
# 10. FINAL CLEANING FOR STATA
# =========================================================

df_final = df_final.filter(
    F.col("wid").isNotNull() &
    F.col("fid").isNotNull() &
    F.col("year").isNotNull() &
    F.col("month").isNotNull() &
    F.col("ln_wage").isNotNull() &
    F.col("edad_norm2").isNotNull() &
    F.col("edad_norm3").isNotNull() &
    F.col("tenure").isNotNull()
)

df_final = df_final.orderBy("wid", "year", "month")

# =========================================================
# 11. SAVE AS SINGLE CSV (NO FOLDER FINAL)
# =========================================================

import os
import shutil

temp_dir = "/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/akm_prepared_csv"
final_path = "/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/akm_prepared_sample.csv"

(
    df_final
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(temp_dir)
)

for file in os.listdir(temp_dir):
    if file.endswith(".csv"):
        shutil.move(
            os.path.join(temp_dir, file),
            final_path
        )

shutil.rmtree(temp_dir)

print("Archivo final guardado en:", final_path)