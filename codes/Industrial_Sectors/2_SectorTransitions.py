import os
import time
import shutil
import glob
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

start_time = time.time()

# =========================================================
# Helper: export Spark DataFrame to a single flat CSV file
# =========================================================
def write_single_csv(df, output_file):
    temp_dir = output_file + "_tmp"

    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", True)
       .csv(temp_dir))

    csv_file = glob.glob(temp_dir + "/part-*.csv")[0]
    shutil.move(csv_file, output_file)
    shutil.rmtree(temp_dir)

# =========================================================
# 1. Spark session
# =========================================================
spark = SparkSession.builder.master("local[*]") \
    .appName("Industrial Sector Transitions") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()

# =========================================================
# 2. Load database
# =========================================================
data = spark.read.csv(
    "/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/processed_data.csv",
    header=True,
    inferSchema=True
)

# Make sure Taxable_Income is numeric
data = data.withColumn("Taxable_Income", F.col("Taxable_Income").cast("double"))

# =========================================================
# 3. Create MIP sector classification from AFC economic activity
# =========================================================
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

# Keep only rows with valid MIP sector
data = data.filter(F.col("MIP_sector_id").isNotNull())

# =========================================================
# 4. Keep only one row per person-period: highest Taxable_Income
# =========================================================
# IMPORTANT:
# Wage_Date is assumed to come as "YYYY-MM"
# Convert to proper monthly date: YYYY-MM-01
# Convert to date and normalize to the first day of the month
data = data.withColumn(
    "Wage_Date",
    F.to_date(F.col("Wage_Date"), "yyyy-MM-dd")
)

data = data.withColumn(
    "Wage_Date",
    F.trunc(F.col("Wage_Date"), "month")
)
w_dedup = Window.partitionBy("ID_Person", "Wage_Date").orderBy(F.col("Taxable_Income").desc())

persons = (
    data.withColumn("_rank", F.row_number().over(w_dedup))
        .filter(F.col("_rank") == 1)
        .select("ID_Person", "Wage_Date", "MIP_sector_id")
        .drop("_rank")
)

# =========================================================
# 5. Generate monthly panel per person between first and last appearance
# =========================================================
person_range = persons.groupBy("ID_Person").agg(
    F.min("Wage_Date").alias("min_date"),
    F.max("Wage_Date").alias("max_date")
)

person_months = (
    person_range.withColumn(
        "Wage_Date",
        F.explode(
            F.sequence(
                F.col("min_date"),
                F.col("max_date"),
                F.expr("interval 1 month")
            )
        )
    )
    .select("ID_Person", "Wage_Date")
)

# Left join with observed sectors
full_panel = person_months.join(
    persons.select("ID_Person", "Wage_Date", F.col("MIP_sector_id").alias("Current_Sector")),
    on=["ID_Person", "Wage_Date"],
    how="left"
)

# =========================================================
# 6. Previous sector and first period flag
# =========================================================
w = Window.partitionBy("ID_Person").orderBy("Wage_Date")

full_panel = (
    full_panel
    .withColumn("Prev_Sector", F.lag("Current_Sector").over(w))
    .withColumn("is_first_period", (F.row_number().over(w) == 1))
)

# =========================================================
# 7. Define State for each person-period
# =========================================================
# State definitions:
# - New Worker: first observed month in database
# - Unemployment: Current_Sector is null
# - Entry: Prev_Sector is null and Current_Sector is not null
# - Otherwise: current MIP sector name
full_panel = full_panel.withColumn(
    "State",
    F.when(F.col("is_first_period"), F.lit("New Worker"))
     .when(F.col("Current_Sector").isNull(), F.lit("Unemployment"))
     .when(F.col("Prev_Sector").isNull() & F.col("Current_Sector").isNotNull(), F.lit("Entry"))
     .otherwise(mip_name_expr[F.col("Current_Sector")])
)

# =========================================================
# 8. Build transitions: State_{t-1} -> State_t
# =========================================================
transitions = (
    full_panel
    .withColumn("State_From", F.lag("State").over(w))
    .withColumn("State_To", F.col("State"))
)

# Exclude invalid transitions
transitions = transitions.filter(
    F.col("State_From").isNotNull() &
    F.col("State_To").isNotNull() &
    (F.col("State_From") != F.lit("New Worker")) &
    (F.col("State_To") != F.lit("New Worker"))
)

# =========================================================
# 9. Aggregate transition probabilities
# =========================================================
transition_counts = (
    transitions
    .groupBy("State_From", "State_To")
    .agg(F.count("*").alias("count"))
)

w_from = Window.partitionBy("State_From")

transition_probs = (
    transition_counts
    .withColumn("total_from", F.sum("count").over(w_from))
    .withColumn("probability", F.round(F.col("count") / F.col("total_from"), 6))
    .orderBy("State_From", "State_To")
)

#print("\n=== Aggregate transition probabilities P(State_To | State_From) ===")
#transition_probs.show(200, truncate=False)

# =========================================================
# 10. Aggregate transition matrix in wide format
# =========================================================
transition_probabilities_wide = (
    transition_probs
    .select("State_From", "State_To", "probability")
    .groupBy("State_From")
    .pivot("State_To")
    .agg(F.first("probability"))
    .fillna(0)
    .orderBy("State_From")
)

# =========================================================
# 11. Transition probabilities by period
# =========================================================
transition_counts_by_period = (
    transitions
    .groupBy("Wage_Date", "State_From", "State_To")
    .agg(F.count("*").alias("count"))
)
#transition_counts_by_period.show(20, truncate=False)

w_from_period = Window.partitionBy("Wage_Date", "State_From")

transition_probs_by_period = (
    transition_counts_by_period
    .withColumn("total_from", F.sum("count").over(w_from_period))
    .withColumn("probability", F.round(F.col("count") / F.col("total_from"), 6))
    .orderBy("Wage_Date", "State_From", "State_To")
)

print("\n=== Transition probabilities by period P(State_To | State_From, Wage_Date) ===")
#transition_probs_by_period.show(20, truncate=False)

# =========================================================
# 12. Transition matrices by period in wide format
# =========================================================
transition_probabilities_by_period_wide = (
    transition_probs_by_period
    .select("Wage_Date", "State_From", "State_To", "probability")
    .groupBy("Wage_Date", "State_From")
    .pivot("State_To")
    .agg(F.first("probability"))
    .fillna(0)
    .orderBy("Wage_Date", "State_From")
)

# =========================================================
# 13. Export results as single flat CSV files
# =========================================================
output_dir = "/Users/valentinavasquez/Documents/GitHub/Public_AFC/output/Industrial_Sectors"
os.makedirs(output_dir, exist_ok=True)

# Aggregate long
output_path_long = os.path.join(output_dir, "transition_probabilities_long.csv")
write_single_csv(transition_probs, output_path_long)
print(f"Saved: {output_path_long}")

# Aggregate wide
output_path_wide = os.path.join(output_dir, "transition_probabilities_wide.csv")
write_single_csv(transition_probabilities_wide, output_path_wide)
print(f"Saved: {output_path_wide}")

# By-period long
output_path_period_long = os.path.join(output_dir, "transition_probabilities_by_period_long.csv")
write_single_csv(transition_probs_by_period, output_path_period_long)
print(f"Saved: {output_path_period_long}")

# By-period wide
output_path_period_wide = os.path.join(output_dir, "transition_probabilities_by_period_wide.csv")
write_single_csv(transition_probabilities_by_period_wide, output_path_period_wide)
print(f"Saved: {output_path_period_wide}")

# =========================================================
# 14. Final timing
# =========================================================
elapsed = time.time() - start_time
print(f"\nDone. Execution time: {elapsed:.2f} seconds")
print(f"All files saved in: {output_dir}")