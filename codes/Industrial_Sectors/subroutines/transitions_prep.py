import glob
import os
import shutil

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DEFAULT_INPUT_PATH = os.path.join(PROJECT_ROOT, "bases", "processed_data.csv")

SECTOR_ID_MAP = {
    1: 1,
    2: 2,
    3: 3,
    4: 4,
    5: 4,
    6: 5,
    7: 6,
    8: 7,
    9: 6,
    10: 7,
    11: 8,
    12: 9,
    13: 10,
    14: 10,
    15: 12,
    16: 11,
    17: 11,
    18: 11,
    19: 11,
    20: 12,
    21: None,
}

SECTOR_NAME_MAP = {
    1: "Agriculture, forestry and fishing",
    2: "Mining",
    3: "Manufacturing",
    4: "Electricity, gas, water and waste management",
    5: "Construction",
    6: "Wholesale and retail trade, accommodation and food services",
    7: "Transport, communications and information services",
    8: "Financial intermediation",
    9: "Real estate and housing services",
    10: "Business services",
    11: "Personal services",
    12: "Public administration",
}

SECTOR_ORDER = [SECTOR_NAME_MAP[i] for i in range(1, 13)]
UNEMPLOYMENT = "Unemployment"
ENTRY = "Entry"
NEW_WORKER = "New Worker"
UNKNOWN_UNEMPLOYMENT = "Unemployment (unknown origin)"

UNEMPLOYMENT_BY_ORIGIN_ORDER = [
    f"Unemployment from {sector}" for sector in SECTOR_ORDER
]

ABBREVIATIONS = {
    "Agriculture, forestry and fishing": "Agriculture",
    "Mining": "Mining",
    "Manufacturing": "Manufacturing",
    "Electricity, gas, water and waste management": "Electricity/gas/water",
    "Construction": "Construction",
    "Wholesale and retail trade, accommodation and food services": "Trade/hotels/rest.",
    "Transport, communications and information services": "Transport/info",
    "Financial intermediation": "Financial",
    "Real estate and housing services": "Real estate",
    "Business services": "Business",
    "Personal services": "Personal",
    "Public administration": "Public admin.",
    "Unemployment": "Unemp.",
    "Entry": "Entry",
    "Unemployment (unknown origin)": "Unemp. unknown",
}

ABBREVIATIONS.update({
    f"Unemployment from {sector}": f"Unemp. from {ABBREVIATIONS[sector]}"
    for sector in SECTOR_ORDER
})


def _create_map(mapping):
    return F.create_map([F.lit(x) for kv in mapping.items() for x in kv])


def sector_name_expr():
    return _create_map(SECTOR_NAME_MAP)


def get_state_order(model):
    model = str(model)

    if model == "13":
        return SECTOR_ORDER + [UNEMPLOYMENT]
    if model == "14":
        return SECTOR_ORDER + [UNEMPLOYMENT, ENTRY]
    if model == "24":
        return SECTOR_ORDER + UNEMPLOYMENT_BY_ORIGIN_ORDER
    if model == "25":
        return SECTOR_ORDER + UNEMPLOYMENT_BY_ORIGIN_ORDER + [ENTRY]

    raise ValueError("model must be one of: 13, 14, 24, 25")


def get_destination_order(model):
    return [state for state in get_state_order(model) if state != ENTRY]


def get_state_abbrev_map(model=None):
    if model is None:
        return ABBREVIATIONS.copy()

    return {
        state: ABBREVIATIONS.get(state, state)
        for state in get_state_order(model)
    }


def write_single_csv(df, output_file):
    temp_dir = output_file + "_tmp"

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(temp_dir)
    )

    csv_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No Spark part CSV found in {temp_dir}")

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    shutil.move(csv_files[0], output_file)
    shutil.rmtree(temp_dir)


def _row_order_map(model):
    return {state: i for i, state in enumerate(get_state_order(model))}


def reorder_transition_csv(csv_path, model="14", by_period=False):
    df = pd.read_csv(csv_path)
    if df.empty:
        df.to_csv(csv_path, index=False)
        return csv_path

    state_col = "State_From" if "State_From" in df.columns else df.columns[1 if by_period else 0]
    lead_cols = ["Wage_Date", state_col] if by_period and "Wage_Date" in df.columns else [state_col]
    lead_cols = [c for c in lead_cols if c in df.columns]

    destination_order = get_destination_order(model)
    destination_cols = [c for c in destination_order if c in df.columns and c not in lead_cols]
    extra_cols = [c for c in df.columns if c not in lead_cols and c not in destination_cols]
    df = df[lead_cols + destination_cols + extra_cols]

    order_map = _row_order_map(model)
    df["_state_order"] = df[state_col].map(order_map).fillna(len(order_map))
    sort_cols = ["_state_order", state_col]
    if by_period and "Wage_Date" in df.columns:
        sort_cols = ["Wage_Date"] + sort_cols

    df = df.sort_values(sort_cols).drop(columns=["_state_order"]).reset_index(drop=True)
    df.to_csv(csv_path, index=False)
    return csv_path


def abbreviate_transition_df(df, model="14", by_period=False):
    out = df.copy()
    out = out.rename(columns={c: ABBREVIATIONS.get(c, c) for c in out.columns})

    state_cols = ["State_From", "State_To"]
    if by_period:
        state_cols = ["State_From", "State_To"]

    for col in state_cols:
        if col in out.columns:
            out[col] = out[col].map(lambda x: ABBREVIATIONS.get(x, x))

    first_state_col = None
    if "State_From" in out.columns:
        first_state_col = "State_From"
    elif len(out.columns) > 0:
        first_state_col = out.columns[1 if by_period and len(out.columns) > 1 else 0]

    if first_state_col and first_state_col in out.columns:
        out[first_state_col] = out[first_state_col].map(lambda x: ABBREVIATIONS.get(x, x))

    return out


def _build_spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("Industrial Sector Transitions")
        .config("spark.executor.memory", "12g")
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )


def _build_base_panel(spark, input_path):
    mip_id_expr = _create_map(SECTOR_ID_MAP)
    mip_name_expr = sector_name_expr()

    data = spark.read.csv(input_path, header=True, inferSchema=True)
    data = (
        data
        .withColumn("Taxable_Income", F.col("Taxable_Income").cast("double"))
        .withColumn("Birthdate", F.to_date(F.col("Birthdate"), "yyyy-MM-dd"))
        .withColumn("Economic_Activity_int", F.col("Economic_Activity").cast("int"))
        .withColumn("MIP_sector_id", mip_id_expr[F.col("Economic_Activity_int")])
        .withColumn(
            "MIP_sector_name",
            F.when(F.col("MIP_sector_id").isNull(), F.lit(None))
            .otherwise(mip_name_expr[F.col("MIP_sector_id")]),
        )
        .drop("Economic_Activity_int")
        .filter(F.col("MIP_sector_id").isNotNull())
        .withColumn("Wage_Date", F.trunc(F.to_date(F.col("Wage_Date"), "yyyy-MM-dd"), "month"))
    )

    w_dedup = Window.partitionBy("ID_Person", "Wage_Date").orderBy(
        F.col("Taxable_Income").desc()
    )

    persons = (
        data
        .withColumn("_rank", F.row_number().over(w_dedup))
        .filter(F.col("_rank") == 1)
        .select("ID_Person", "Wage_Date", "MIP_sector_id", "Birthdate")
        .drop("_rank")
    )

    global_max_date = persons.agg(F.max("Wage_Date").alias("global_max_date")).collect()[0][
        "global_max_date"
    ]

    person_range = persons.groupBy("ID_Person").agg(
        F.min("Wage_Date").alias("min_date"),
        F.first("Birthdate", ignorenulls=True).alias("Birthdate"),
    )

    person_range = (
        person_range
        .withColumn("date_60", F.add_months(F.col("Birthdate"), 60 * 12))
        .withColumn("date_60_month", F.trunc(F.col("date_60"), "month"))
        .withColumn("panel_end_date", F.least(F.lit(global_max_date), F.col("date_60_month")))
        .filter(F.col("min_date") <= F.col("panel_end_date"))
    )

    person_months = (
        person_range
        .withColumn(
            "Wage_Date",
            F.explode(
                F.sequence(
                    F.col("min_date"),
                    F.col("panel_end_date"),
                    F.expr("interval 1 month"),
                )
            ),
        )
        .select("ID_Person", "Wage_Date")
    )

    full_panel = person_months.join(
        persons.select(
            "ID_Person",
            "Wage_Date",
            F.col("MIP_sector_id").alias("Current_Sector"),
        ),
        on=["ID_Person", "Wage_Date"],
        how="left",
    )

    w = Window.partitionBy("ID_Person").orderBy("Wage_Date")

    return (
        full_panel
        .withColumn("Prev_Sector", F.lag("Current_Sector").over(w))
        .withColumn("is_first_period", F.row_number().over(w) == 1)
    )


def _build_transitions(panel, state_col):
    w = Window.partitionBy("ID_Person").orderBy("Wage_Date")

    return (
        panel
        .withColumn("State_From", F.lag(state_col).over(w))
        .withColumn("State_To", F.col(state_col))
        .filter(
            F.col("State_From").isNotNull()
            & F.col("State_To").isNotNull()
            & (F.col("State_From") != F.lit(NEW_WORKER))
            & (F.col("State_To") != F.lit(NEW_WORKER))
        )
        .select("ID_Person", "Wage_Date", "State_From", "State_To")
    )


def prepare(input_path=None, spark=None):
    spark = spark or _build_spark()
    input_path = input_path or DEFAULT_INPUT_PATH
    mip_name_expr = sector_name_expr()

    full_panel = _build_base_panel(spark, input_path)

    panel_13 = full_panel.withColumn(
        "State_13",
        F.when(F.col("is_first_period"), F.lit(NEW_WORKER))
        .when(F.col("Current_Sector").isNull(), F.lit(UNEMPLOYMENT))
        .otherwise(mip_name_expr[F.col("Current_Sector")]),
    )

    panel_14 = full_panel.withColumn(
        "State_14",
        F.when(F.col("is_first_period"), F.lit(NEW_WORKER))
        .when(F.col("Current_Sector").isNull(), F.lit(UNEMPLOYMENT))
        .when(
            F.col("Prev_Sector").isNull() & F.col("Current_Sector").isNotNull(),
            F.lit(ENTRY),
        )
        .otherwise(mip_name_expr[F.col("Current_Sector")]),
    )

    w_fill = (
        Window.partitionBy("ID_Person")
        .orderBy("Wage_Date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    panel_with_origin = full_panel.withColumn(
        "Last_Known_Sector",
        F.last(F.col("Current_Sector"), ignorenulls=True).over(w_fill),
    )

    panel_24 = panel_with_origin.withColumn(
        "State_24",
        F.when(F.col("is_first_period"), F.lit(NEW_WORKER))
        .when(F.col("Current_Sector").isNotNull(), mip_name_expr[F.col("Current_Sector")])
        .when(
            F.col("Last_Known_Sector").isNotNull(),
            F.concat(F.lit("Unemployment from "), mip_name_expr[F.col("Last_Known_Sector")]),
        )
        .otherwise(F.lit(UNKNOWN_UNEMPLOYMENT)),
    )

    panel_25 = panel_with_origin.withColumn(
        "State_25",
        F.when(F.col("is_first_period"), F.lit(NEW_WORKER))
        .when(
            F.col("Prev_Sector").isNull() & F.col("Current_Sector").isNotNull(),
            F.lit(ENTRY),
        )
        .when(F.col("Current_Sector").isNotNull(), mip_name_expr[F.col("Current_Sector")])
        .when(
            F.col("Last_Known_Sector").isNotNull(),
            F.concat(F.lit("Unemployment from "), mip_name_expr[F.col("Last_Known_Sector")]),
        )
        .otherwise(F.lit(UNKNOWN_UNEMPLOYMENT)),
    )

    transitions_13 = _build_transitions(panel_13, "State_13")
    transitions_14 = _build_transitions(panel_14, "State_14")
    transitions_24 = _build_transitions(panel_24, "State_24")
    transitions_25 = _build_transitions(panel_25, "State_25")

    return {
        "spark": spark,
        "full_panel": full_panel,
        "transitions": transitions_14,
        "transitions_13": transitions_13,
        "transitions_14": transitions_14,
        "transitions_24": transitions_24,
        "transitions_25": transitions_25,
    }
