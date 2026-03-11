import os
import time
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

start_time = time.time()

# =========================================================
# 1. Spark session
# =========================================================
spark = SparkSession.builder.master("local[*]") \
    .appName("Skill Transition Probabilities") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
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
# 3. Convert Wage_Date to monthly date
# =========================================================
data = data.withColumn(
    "Wage_Date",
    F.to_date(F.col("Wage_Date"), "yyyy-MM-dd")
)

data = data.withColumn(
    "Wage_Date",
    F.trunc(F.col("Wage_Date"), "month")
)

# =========================================================
# 4. Keep only one row per person-period: highest Taxable_Income
# =========================================================
w_dedup = Window.partitionBy("ID_Person", "Wage_Date").orderBy(F.col("Taxable_Income").desc())

persons = (
    data.withColumn("_rank", F.row_number().over(w_dedup))
        .filter(F.col("_rank") == 1)
        .select("ID_Person", "Wage_Date", "Skill_Broad", "Skill_Strict")
        .drop("_rank")
        .withColumn("Observed", F.lit(1))
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

# Left join with observed person-month data
full_panel = person_months.join(
    persons.select("ID_Person", "Wage_Date", "Skill_Broad", "Skill_Strict", "Observed"),
    on=["ID_Person", "Wage_Date"],
    how="left"
)

# =========================================================
# 6. Employment state
# =========================================================
full_panel = full_panel.withColumn(
    "State",
    F.when(F.col("Observed").isNull(), F.lit("Unemployment"))
     .otherwise(F.lit("Employment"))
)

# =========================================================
# 7. Fill skills forward within each person
# =========================================================
w_fill = Window.partitionBy("ID_Person").orderBy("Wage_Date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

full_panel = full_panel.withColumn(
    "Skill_Broad_filled",
    F.last("Skill_Broad", ignorenulls=True).over(w_fill)
)

full_panel = full_panel.withColumn(
    "Skill_Strict_filled",
    F.last("Skill_Strict", ignorenulls=True).over(w_fill)
)

# =========================================================
# 8. Helper function to compute and export transitions
# =========================================================
def compute_transition_outputs(panel_df, skill_col, skill_value, output_prefix, output_dir):
    """
    panel_df: full panel
    skill_col: Skill_Broad_filled or Skill_Strict_filled
    skill_value: Skilled / Unskilled
    output_prefix: prefix for exported filenames
    output_dir: export folder
    """

    os.makedirs(output_dir, exist_ok=True)

    # Keep only person-months belonging to this skill group
    skill_panel = panel_df.filter(F.col(skill_col) == skill_value)

    # Transition definition
    w = Window.partitionBy("ID_Person").orderBy("Wage_Date")

    transitions = (
        skill_panel
        .withColumn("State_From", F.lag("State").over(w))
        .withColumn("State_To", F.col("State"))
        .filter(F.col("State_From").isNotNull())
    )

    # -------------------------
    # Aggregate probabilities
    # -------------------------
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

    print(f"\n=== Aggregate transition probabilities for {skill_value} ({skill_col}) ===")
    transition_probs.show(20, truncate=False)

    # Wide aggregate matrix
    transition_probs_wide = (
        transition_probs
        .select("State_From", "State_To", "probability")
        .groupBy("State_From")
        .pivot("State_To")
        .agg(F.first("probability"))
        .fillna(0)
        .orderBy("State_From")
    )

    # -------------------------
    # By-period probabilities
    # -------------------------
    transition_counts_by_period = (
        transitions
        .groupBy("Wage_Date", "State_From", "State_To")
        .agg(F.count("*").alias("count"))
    )

    w_from_period = Window.partitionBy("Wage_Date", "State_From")

    transition_probs_by_period = (
        transition_counts_by_period
        .withColumn("total_from", F.sum("count").over(w_from_period))
        .withColumn("probability", F.round(F.col("count") / F.col("total_from"), 6))
        .orderBy("Wage_Date", "State_From", "State_To")
    )

    print(f"\n=== By-period transition probabilities for {skill_value} ({skill_col}) ===")
    transition_probs_by_period.show(20, truncate=False)

    # Wide by-period matrix
    transition_probs_by_period_wide = (
        transition_probs_by_period
        .select("Wage_Date", "State_From", "State_To", "probability")
        .groupBy("Wage_Date", "State_From")
        .pivot("State_To")
        .agg(F.first("probability"))
        .fillna(0)
        .orderBy("Wage_Date", "State_From")
    )

    # -------------------------
    # Export
    # -------------------------
    transition_probs.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(output_dir, f"{output_prefix}_{skill_value.lower()}_long")
    )

    transition_probs_wide.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(output_dir, f"{output_prefix}_{skill_value.lower()}_wide")
    )

    transition_probs_by_period.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(output_dir, f"{output_prefix}_{skill_value.lower()}_by_period_long")
    )

    transition_probs_by_period_wide.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(output_dir, f"{output_prefix}_{skill_value.lower()}_by_period_wide")
    )

# =========================================================
# 9. Run for Skill_Broad
# =========================================================
output_dir = "/Users/valentinavasquez/Documents/GitHub/Public_AFC/output/Skilled_Unskilled"

for skill in ["Skilled", "Unskilled"]:
    compute_transition_outputs(
        panel_df=full_panel,
        skill_col="Skill_Broad_filled",
        skill_value=skill,
        output_prefix="transition_probabilities_broad",
        output_dir=output_dir
    )

# =========================================================
# 10. Run for Skill_Strict
# =========================================================
for skill in ["Skilled", "Unskilled"]:
    compute_transition_outputs(
        panel_df=full_panel,
        skill_col="Skill_Strict_filled",
        skill_value=skill,
        output_prefix="transition_probabilities_strict",
        output_dir=output_dir
    )

# =========================================================
# 11. Final timing
# =========================================================
elapsed = time.time() - start_time
print(f"\nDone. Execution time: {elapsed:.2f} seconds")
print(f"All files saved in: {output_dir}")