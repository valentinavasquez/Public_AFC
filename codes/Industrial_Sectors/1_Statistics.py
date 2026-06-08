import os
import time
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.filters.hp_filter import hpfilter

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
    '/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/processed_data.csv', header=True, inferSchema=True)

# Crear columnas numéricas 'year' y 'month' a partir de 'Wage_Date'
# Suponiendo que Wage_Date es tipo string en formato 'YYYY-MM' o 'YYYY-MM-DD'
data = data.withColumn('Wage_Date_dt', F.to_date(F.col('Wage_Date'), 'yyyy-MM-dd'))
data = data.withColumn('year', F.year(F.col('Wage_Date_dt')))
data = data.withColumn('month', F.month(F.col('Wage_Date_dt')))
data = data.drop('Wage_Date_dt')

# Cargar ipc_clean.csv y hacer join por year y month
ipc = spark.read.csv('/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/ipc_clean.csv', header=True, inferSchema=True)
data = data.join(ipc, on=['year', 'month'], how='left')

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
    6: "Wholesale and retail trade, accommodation and food services",
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

#### 1. COUNT OF WORKERS

#By period and MIP sector
count_by_sector = data.groupBy('Wage_Date', 'MIP_sector_id', 'MIP_sector_name') \
    .agg(F.countDistinct('ID_Person').alias('Count_Workers')) \
    .orderBy('Wage_Date', 'MIP_sector_id')
# Cantidad de trabajadores por periodo (total)
count_total = data.groupBy('Wage_Date', 'year', 'month') \
    .agg(F.countDistinct('ID_Person').alias('Count_Workers')) \
    .orderBy('Wage_Date')
#By MIP sector (aggregate)
agg_count_by_sector = data.groupBy('MIP_sector_id', 'MIP_sector_name') \
    .agg(F.countDistinct('ID_Person').alias('Count_Workers')) \
    .orderBy('MIP_sector_id')

# Total count of workers by sector (aggregate)
agg_count_total = data.select(F.countDistinct('ID_Person')).collect()[0][0]

#### 2. SHARES OF WORKERS BY PERIOD AND MIP SECTOR (%)
# Calculate shares
shares_by_sector = count_by_sector.join(count_total, on=['Wage_Date']) \
    .withColumn('Share', F.round(F.col('Count_Workers') / F.col('Count_Workers') * 100, 2)) \
    .orderBy('Wage_Date', 'MIP_sector_id')

# Share of workers by sector (aggregate)
agg_shares_by_sector = agg_count_by_sector \
    .withColumn('Share', F.round(F.col('Count_Workers') / F.lit(agg_count_total) * 100, 2)) \
    .orderBy('MIP_sector_id')

#### 3. NOMINAL WAGES
#By period and MIP sector
avg_wage_by_sector = data.groupBy('Wage_Date', 'MIP_sector_id', 'MIP_sector_name') \
    .agg(F.round(F.avg('Taxable_Income'), 2).alias('Avg_Taxable_Income')) \
    .orderBy('Wage_Date', 'MIP_sector_id')

#By period (aggregate)
# Average wage by period (aggregate)
avg_wage_total = data.groupBy('Wage_Date', 'year', 'month') \
    .agg(F.round(F.avg('Taxable_Income'), 2).alias('Avg_Taxable_Income')) \
    .orderBy('Wage_Date')

#By MIP sector (aggregate)
agg_avg_wage_by_sector = data.groupBy('MIP_sector_id', 'MIP_sector_name') \
    .agg(F.round(F.avg('Taxable_Income'), 2).alias('Avg_Taxable_Income')) \
    .orderBy('MIP_sector_id')

# Average wage total (aggregate)
agg_avg_wage_total = data.agg(F.round(F.avg('Taxable_Income'), 2).alias('Avg_Taxable_Income')).collect()[0][0]


#### 4. REAL WAGES

# Create real wage column by adjusting taxable income with CPI
data = data.withColumn('Real_Wage', F.col('Taxable_Income') / F.col('ipc_def') * 100)

# By period and MIP sector
real_avg_wage_by_sector = data.groupBy('Wage_Date', 'MIP_sector_id', 'MIP_sector_name') \
    .agg(F.round(F.avg('Real_Wage'), 2).alias('Real_Avg_Wage')) \
    .orderBy('Wage_Date', 'MIP_sector_id')

# By period (aggregate)
real_avg_wage_total = data.groupBy('Wage_Date').agg(F.round(F.avg('Real_Wage'), 2).alias('Real_Avg_Wage')).orderBy('Wage_Date')

# By MIP sector (aggregate)
agg_real_avg_wage_by_sector = data.groupBy('MIP_sector_id', 'MIP_sector_name') \
    .agg(F.round(F.avg('Real_Wage'), 2).alias('Real_Avg_Wage')) \
    .orderBy('MIP_sector_id')

# Real average wage total (aggregate)
agg_real_avg_wage_total = data.agg(F.round(F.avg('Real_Wage'), 2).alias('Real_Avg_Wage')).collect()[0][0]


#5. DESEASONALIZATION

import sys
from statsmodels.tsa.seasonal import seasonal_decompose

# === DESESTACIONALIZACIÓN DE SALARIOS REALES ===
print("\n[INFO] Desestacionalizando salarios reales agregados y por sector...")

def desestacionalizar(df_pandas, value_col, date_cols=["year", "month"]):
    """Desestacionaliza una serie mensual usando STL decomposition."""
    df = df_pandas.copy()
    df["date"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str) + "-01")
    df = df.sort_values("date").set_index("date")
    if len(df) >= 24:
        result = seasonal_decompose(df[value_col], model="additive", period=12)
        df[f"{value_col}_sa"] = df[value_col] - result.seasonal
        df[f"{value_col}_trend"] = result.trend
    else:
        df[f"{value_col}_sa"] = df[value_col]
        df[f"{value_col}_trend"] = df[value_col]
    df = df.reset_index()
    return df


# --- Asegurar columnas year y month en los DataFrames: Create columns year and month otherwise ---
def ensure_year_month(df, date_col="Wage_Date"):
    if "year" not in df.columns or "month" not in df.columns:
        df["year"] = pd.to_datetime(df[date_col]).dt.year
        df["month"] = pd.to_datetime(df[date_col]).dt.month
    return df


# 5.1 COUNT WORKERS DESEASONALIZATION
# Desestacionalizar count total
count_total_sa = count_total.toPandas()
count_total_sa = ensure_year_month(count_total_sa, "Wage_Date")
count_total_sa = desestacionalizar(count_total_sa, "Count_Workers")

# Desestacionalizar count_by_sector
count_by_sector_sa = count_by_sector.toPandas()
count_by_sector_sa = ensure_year_month(count_by_sector_sa, "Wage_Date")
dfs_count_by_sector_sa = []
for sector_id, sector_name in count_by_sector_sa[["MIP_sector_id", "MIP_sector_name"]].drop_duplicates().values:
    sub = count_by_sector_sa[(count_by_sector_sa["MIP_sector_id"] == sector_id) & (count_by_sector_sa["MIP_sector_name"] == sector_name)].copy()
    sub = ensure_year_month(sub, "Wage_Date")
    sub_sa = desestacionalizar(sub, "Count_Workers").reset_index()
    sub_sa["MIP_sector_id"] = sector_id
    sub_sa["MIP_sector_name"] = sector_name
    dfs_count_by_sector_sa.append(sub_sa)
count_by_sector_sa = pd.concat(dfs_count_by_sector_sa, ignore_index=True)

# 5.2 NOMINAL WAGES DESEASONALIZATION
print("\n[INFO] Deseasonalizing nominal wages...")

# Deseasonalizing avg_wage_by_sector
pdf_nominal_sector = avg_wage_by_sector.toPandas()
pdf_nominal_sector = ensure_year_month(pdf_nominal_sector, "Wage_Date")
dfs_nominal_sector_sa = []
for sector_id, sector_name in pdf_nominal_sector[["MIP_sector_id", "MIP_sector_name"]].drop_duplicates().values:
    sub = pdf_nominal_sector[(pdf_nominal_sector["MIP_sector_id"] == sector_id) & (pdf_nominal_sector["MIP_sector_name"] == sector_name)].copy()
    sub = ensure_year_month(sub, "Wage_Date")
    sub_sa = desestacionalizar(sub, "Avg_Taxable_Income").reset_index()
    sub_sa["MIP_sector_id"] = sector_id
    sub_sa["MIP_sector_name"] = sector_name
    dfs_nominal_sector_sa.append(sub_sa)
avg_wage_by_sector_sa = pd.concat(dfs_nominal_sector_sa, ignore_index=True)

# Deseasonalizing avg_wage_total
avg_wage_total_sa = avg_wage_total.toPandas()
avg_wage_total_sa = ensure_year_month(avg_wage_total_sa, "Wage_Date")
avg_wage_total_sa = desestacionalizar(avg_wage_total_sa, "Avg_Taxable_Income")
avg_wage_total_sa = avg_wage_total_sa.reset_index()

# 5.3 REAL WAGES DESEASONALIZATION

# Desestacionalizar salario real por sector
real_avg_wage_by_sector_sa = real_avg_wage_by_sector.toPandas()
real_avg_wage_by_sector_sa = ensure_year_month(real_avg_wage_by_sector_sa, "Wage_Date")
dfs_real_sector_sa = []
for sector_id, sector_name in real_avg_wage_by_sector_sa[["MIP_sector_id", "MIP_sector_name"]].drop_duplicates().values:
    sub = real_avg_wage_by_sector_sa[(real_avg_wage_by_sector_sa["MIP_sector_id"] == sector_id) & (real_avg_wage_by_sector_sa["MIP_sector_name"] == sector_name)].copy()
    sub = ensure_year_month(sub, "Wage_Date")
    sub_sa = desestacionalizar(sub, "Real_Avg_Wage").reset_index()
    sub_sa["MIP_sector_id"] = sector_id
    sub_sa["MIP_sector_name"] = sector_name
    dfs_real_sector_sa.append(sub_sa)
real_avg_wage_by_sector_sa = pd.concat(dfs_real_sector_sa, ignore_index=True)

# Desestacionalizar salario real agregado
real_avg_wage_total_sa = real_avg_wage_total.toPandas()
real_avg_wage_total_sa = ensure_year_month(real_avg_wage_total_sa, "Wage_Date")
real_avg_wage_total_sa = desestacionalizar(real_avg_wage_total_sa, "Real_Avg_Wage")


# 5.4 3-MONTH MOVING AVERAGE (NOMINAL AND REAL WAGES)
print("\n[INFO] Calculating 3-month moving averages for nominal and real wages...")

# Nominal wages - by sector
pdf_nominal_sector_ma3 = avg_wage_by_sector.toPandas()
pdf_nominal_sector_ma3 = ensure_year_month(pdf_nominal_sector_ma3, "Wage_Date")
pdf_nominal_sector_ma3 = pdf_nominal_sector_ma3.sort_values(["MIP_sector_id", "Wage_Date"])
pdf_nominal_sector_ma3["Avg_Taxable_Income_MA3"] = (
    pdf_nominal_sector_ma3.groupby("MIP_sector_id")["Avg_Taxable_Income"]
    .transform(lambda x: x.rolling(3, min_periods=3).mean())
)

# Nominal wages - total
avg_wage_total_ma3 = avg_wage_total.toPandas()
avg_wage_total_ma3 = ensure_year_month(avg_wage_total_ma3, "Wage_Date")
avg_wage_total_ma3 = avg_wage_total_ma3.sort_values("Wage_Date")
avg_wage_total_ma3["Avg_Taxable_Income_MA3"] = avg_wage_total_ma3["Avg_Taxable_Income"].rolling(3, min_periods=3).mean()

# Real wages - by sector
pdf_real_sector_ma3 = real_avg_wage_by_sector.toPandas()
pdf_real_sector_ma3 = ensure_year_month(pdf_real_sector_ma3, "Wage_Date")
pdf_real_sector_ma3 = pdf_real_sector_ma3.sort_values(["MIP_sector_id", "Wage_Date"])
pdf_real_sector_ma3["Real_Avg_Wage_MA3"] = (
    pdf_real_sector_ma3.groupby("MIP_sector_id")["Real_Avg_Wage"]
    .transform(lambda x: x.rolling(3, min_periods=3).mean())
)

# Real wages - total
real_avg_wage_total_ma3 = real_avg_wage_total.toPandas()
real_avg_wage_total_ma3 = ensure_year_month(real_avg_wage_total_ma3, "Wage_Date")
real_avg_wage_total_ma3 = real_avg_wage_total_ma3.sort_values("Wage_Date")
real_avg_wage_total_ma3["Real_Avg_Wage_MA3"] = real_avg_wage_total_ma3["Real_Avg_Wage"].rolling(3, min_periods=3).mean()


# 5.5 DESEASONALIZATION OF 3-MONTH MOVING AVERAGES
print("\n[INFO] Deseasonalizing 3-month moving averages...")

# Nominal MA3 - by sector
dfs_nominal_sector_ma3_sa = []
for sector_id, sector_name in pdf_nominal_sector_ma3[["MIP_sector_id", "MIP_sector_name"]].drop_duplicates().values:
    sub = pdf_nominal_sector_ma3[
        (pdf_nominal_sector_ma3["MIP_sector_id"] == sector_id) &
        (pdf_nominal_sector_ma3["MIP_sector_name"] == sector_name)
    ].dropna(subset=["Avg_Taxable_Income_MA3"]).copy()
    sub_sa = desestacionalizar(sub, "Avg_Taxable_Income_MA3").reset_index()
    sub_sa["MIP_sector_id"] = sector_id
    sub_sa["MIP_sector_name"] = sector_name
    dfs_nominal_sector_ma3_sa.append(sub_sa)
avg_wage_by_sector_ma3_sa = pd.concat(dfs_nominal_sector_ma3_sa, ignore_index=True)

# Nominal MA3 - total
avg_wage_total_ma3_sa = avg_wage_total_ma3.dropna(subset=["Avg_Taxable_Income_MA3"]).copy()
avg_wage_total_ma3_sa = desestacionalizar(avg_wage_total_ma3_sa, "Avg_Taxable_Income_MA3")
avg_wage_total_ma3_sa = avg_wage_total_ma3_sa.reset_index()

# Real MA3 - by sector
dfs_real_sector_ma3_sa = []
for sector_id, sector_name in pdf_real_sector_ma3[["MIP_sector_id", "MIP_sector_name"]].drop_duplicates().values:
    sub = pdf_real_sector_ma3[
        (pdf_real_sector_ma3["MIP_sector_id"] == sector_id) &
        (pdf_real_sector_ma3["MIP_sector_name"] == sector_name)
    ].dropna(subset=["Real_Avg_Wage_MA3"]).copy()
    sub_sa = desestacionalizar(sub, "Real_Avg_Wage_MA3").reset_index()
    sub_sa["MIP_sector_id"] = sector_id
    sub_sa["MIP_sector_name"] = sector_name
    dfs_real_sector_ma3_sa.append(sub_sa)
real_avg_wage_by_sector_ma3_sa = pd.concat(dfs_real_sector_ma3_sa, ignore_index=True)

# Real MA3 - total
real_avg_wage_total_ma3_sa = real_avg_wage_total_ma3.dropna(subset=["Real_Avg_Wage_MA3"]).copy()
real_avg_wage_total_ma3_sa = desestacionalizar(real_avg_wage_total_ma3_sa, "Real_Avg_Wage_MA3")
real_avg_wage_total_ma3_sa = real_avg_wage_total_ma3_sa.reset_index()


# 6. GROWTH RATES AND INFLATION

print("\n[INFO] Calculating growth rates of employment (seasonally adjusted)...")

#All variables are seasonally adjusted prior to transformation into growth rates.

# 6. GROWTH RATES AND INFLATION

print("\n[INFO] Calculating monthly and annual growth rates...")

def add_growth_rates(
    df,
    value_col,
    monthly_col,
    annual_col,
    group_col=None,
    date_col="date"
):
    """
    Adds monthly and annual percentage growth rates.

    Parameters
    ----------
    df : pandas.DataFrame
    value_col : str
        Column used to compute growth.
    monthly_col : str
        Name of monthly growth column.
    annual_col : str
        Name of annual growth column.
    group_col : str or None
        Grouping variable for sector-level series.
    date_col : str
        Date column used for sorting.
    """
    df = df.copy()

    if group_col is None:
        df = df.sort_values(date_col)
        df[monthly_col] = df[value_col].pct_change() * 100
        df[annual_col] = df[value_col].pct_change(periods=12) * 100

    else:
        df = df.sort_values([group_col, date_col])
        df[monthly_col] = (
            df.groupby(group_col)[value_col]
            .pct_change() * 100
        )
        df[annual_col] = (
            df.groupby(group_col)[value_col]
            .pct_change(periods=12) * 100
        )

    return df

    # 6.1 Employment growth

count_sector_sa_growth = add_growth_rates(
    df=count_by_sector_sa,
    value_col="Count_Workers_sa",
    monthly_col="Monthly_Emp_Growth",
    annual_col="Annual_Emp_Growth",
    group_col="MIP_sector_id"
)

count_total_sa_growth = add_growth_rates(
    df=count_total_sa,
    value_col="Count_Workers_sa",
    monthly_col="Monthly_Emp_Growth",
    annual_col="Annual_Emp_Growth"
)


# 6.2 Nominal wage growth

avg_wage_by_sector_sa_growth = add_growth_rates(
    df=avg_wage_by_sector_sa,
    value_col="Avg_Taxable_Income_sa",
    monthly_col="Nominal_Wage_Growth_SA",
    annual_col="Nominal_Wage_Growth_SA_Annual",
    group_col="MIP_sector_id"
)

avg_wage_total_sa_growth = add_growth_rates(
    df=avg_wage_total_sa,
    value_col="Avg_Taxable_Income_sa",
    monthly_col="Nominal_Wage_Growth_SA",
    annual_col="Nominal_Wage_Growth_SA_Annual"
)


# 6.3 Real wage growth

real_avg_wage_by_sector_sa_growth = add_growth_rates(
    df=real_avg_wage_by_sector_sa,
    value_col="Real_Avg_Wage_sa",
    monthly_col="Real_Wage_Growth_SA",
    annual_col="Real_Wage_Growth_SA_Annual",
    group_col="MIP_sector_id"
)

real_avg_wage_total_sa_growth = add_growth_rates(
    df=real_avg_wage_total_sa,
    value_col="Real_Avg_Wage_sa",
    monthly_col="Real_Wage_Growth_SA",
    annual_col="Real_Wage_Growth_SA_Annual"
)


# 6.4 3-month moving average growth, seasonally adjusted

avg_wage_by_sector_ma3_sa_growth = add_growth_rates(
    df=avg_wage_by_sector_ma3_sa,
    value_col="Avg_Taxable_Income_MA3_sa",
    monthly_col="Nominal_Wage_MA3_Growth_SA",
    annual_col="Nominal_Wage_MA3_Growth_SA_Annual",
    group_col="MIP_sector_id"
)

avg_wage_total_ma3_sa_growth = add_growth_rates(
    df=avg_wage_total_ma3_sa,
    value_col="Avg_Taxable_Income_MA3_sa",
    monthly_col="Nominal_Wage_MA3_Growth_SA",
    annual_col="Nominal_Wage_MA3_Growth_SA_Annual"
)

real_avg_wage_by_sector_ma3_sa_growth = add_growth_rates(
    df=real_avg_wage_by_sector_ma3_sa,
    value_col="Real_Avg_Wage_MA3_sa",
    monthly_col="Real_Wage_MA3_Growth_SA",
    annual_col="Real_Wage_MA3_Growth_SA_Annual",
    group_col="MIP_sector_id"
)

real_avg_wage_total_ma3_sa_growth = add_growth_rates(
    df=real_avg_wage_total_ma3_sa,
    value_col="Real_Avg_Wage_MA3_sa",
    monthly_col="Real_Wage_MA3_Growth_SA",
    annual_col="Real_Wage_MA3_Growth_SA_Annual"
)

# 7. VOLATILITY

# 7.1 VOLATILITY EMPLOYMENT (SEASONALLY ADJUSTED)
print("\n[INFO] Calculating employment volatility (seasonally adjusted)...")

# count_by_sector_sa
vol_emp_sector_sa = count_by_sector_sa.groupby("MIP_sector_id")["Count_Workers_sa"].std().reset_index().rename(columns={"Count_Workers_sa": "Vol_Emp_SA"})

# count_total_sa
vol_emp_total_sa = count_total_sa["Count_Workers_sa"].std()

# 7.2 VOLATILITY NOMINAL WAGES (SEASONALLY ADJUSTED)
print("\n[INFO] Calculating nominal wage volatility...")

# avg_wage_by_sector_sa 
vol_nominal_wage_sector_sa = avg_wage_by_sector_sa.groupby("MIP_sector_id")["Avg_Taxable_Income_sa"].std().reset_index().rename(columns={"Avg_Taxable_Income_sa": "Vol_Nominal_Wage_SA"})

# avg_wage_total_sa
vol_nominal_wage_total_sa = avg_wage_total_sa["Avg_Taxable_Income_sa"].std()


# 7.3 VOLATILITY REAL WAGES (SEASONALLY ADJUSTED)
print("\n[INFO] Calculating real wage volatility...")    

# real_avg_wage_by_sector_sa
vol_real_wage_sector_sa = real_avg_wage_by_sector_sa.groupby("MIP_sector_id")["Real_Avg_Wage_sa"].std().reset_index().rename(columns={"Real_Avg_Wage_sa": "Vol_Real_Wage_SA"})

# real_avg_wage_total_sa
vol_real_wage_total_sa = real_avg_wage_total_sa["Real_Avg_Wage_sa"].std()

# 7.4 VOLATILITY EMPLOYMENT GROWTH (SEASONALLY ADJUSTED)
print("\n[INFO] Calculating employment growth volatility (seasonally adjusted)...")
# count_sector_sa_growth
vol_emp_growth_sector_sa = count_sector_sa_growth.groupby("MIP_sector_id")["Monthly_Emp_Growth"].std().reset_index().rename(columns={"Monthly_Emp_Growth": "Vol_Emp_Growth_SA"})    
# count_total_sa_growth
vol_emp_growth_total_sa = count_total_sa_growth["Monthly_Emp_Growth"].std()

#7.5 VOLATILITY NOMINAL WAGE GROWTH (SEASONALLY ADJUSTED)
print("\n[INFO] Calculating nominal wage growth volatility (seasonally adjusted)...")
# avg_wage_by_sector_sa_growth
vol_nominal_wage_growth_sector_sa = avg_wage_by_sector_sa_growth.groupby("MIP_sector_id")["Nominal_Wage_Growth_SA"].std().reset_index().rename(columns={"Nominal_Wage_Growth_SA": "Vol_Nominal_Wage_Growth_SA"})
# avg_wage_total_sa_growth
vol_nominal_wage_growth_total_sa = avg_wage_total_sa_growth["Nominal_Wage_Growth_SA"].std() 

# 7.6 VOLATILITY REAL WAGE GROWTH (SEASONALLY ADJUSTED)
print("\n[INFO] Calculating real wage growth volatility (seasonally adjusted)...")
# real_avg_wage_by_sector_sa_growth
vol_real_wage_growth_sector_sa = real_avg_wage_by_sector_sa_growth.groupby("MIP_sector_id")["Real_Wage_Growth_SA"].std().reset_index().rename(columns={"Real_Wage_Growth_SA": "Vol_Real_Wage_Growth_SA"})       
# real_avg_wage_total_sa_growth
vol_real_wage_growth_total_sa = real_avg_wage_total_sa_growth["Real_Wage_Growth_SA"].std()      


# 8. HP filter
from statsmodels.tsa.filters.hp_filter import hpfilter
print("\n[INFO] Aplicando filtro HP a salarios y empleo desestacionalizados...")

# --- Helper para aplicar HP filter y devolver ciclo ---
def add_hp_cycle(df, value_col, lamb=129600, min_obs=24):
    """Apply HP filter and return dataframe with a new '<value_col>_cycle' column.

    min_obs: minimal number of non-missing observations required to run HP filter
    (useful to relax threshold for shorter series).
    """
    df = df.copy()
    cycle_col = value_col + "_cycle"
    df[cycle_col] = np.nan  # inicializar con NaN
    sector_cols = [c for c in ["MIP_sector_id", "MIP_sector_name"] if c in df.columns]
    if sector_cols:
        for key, group in df.groupby(sector_cols):
            mask = group[value_col].notna()
            valid = group.loc[mask, value_col]
            if len(valid) >= min_obs:
                cycle, trend = hpfilter(valid, lamb=lamb)
                df.loc[valid.index, cycle_col] = cycle
    else:
        mask = df[value_col].notna()
        valid = df.loc[mask, value_col]
        if len(valid) >= min_obs:
            cycle, trend = hpfilter(valid, lamb=lamb)
            df.loc[valid.index, cycle_col] = cycle
    return df

# --- Salarios nominales desestacionalizados ---
avg_wage_by_sector_sa_hp = add_hp_cycle(avg_wage_by_sector_sa, "Avg_Taxable_Income_sa")
avg_wage_total_sa_hp = add_hp_cycle(avg_wage_total_sa, "Avg_Taxable_Income_sa")

# --- Salarios reales desestacionalizados ---
real_avg_wage_by_sector_sa_hp = add_hp_cycle(real_avg_wage_by_sector_sa, "Real_Avg_Wage_sa")
real_avg_wage_total_sa_hp = add_hp_cycle(real_avg_wage_total_sa, "Real_Avg_Wage_sa")

# --- Empleo desestacionalizado ---
count_by_sector_sa_hp = add_hp_cycle(count_by_sector_sa, "Count_Workers_sa")
count_total_sa_hp = add_hp_cycle(count_total_sa, "Count_Workers_sa")

# --- HP cycles para crecimientos anuales (salario nominal, salario real, empleo) ---
# Aplicamos el HP sobre las series de crecimiento anual (tienen frecuencia mensual pero representan tasas year-on-year)
nominal_growth_by_sector_hp = add_hp_cycle(avg_wage_by_sector_sa_growth, "Nominal_Wage_Growth_SA_Annual")
nominal_growth_total_hp = add_hp_cycle(avg_wage_total_sa_growth, "Nominal_Wage_Growth_SA_Annual")

real_growth_by_sector_hp = add_hp_cycle(real_avg_wage_by_sector_sa_growth, "Real_Wage_Growth_SA_Annual")
real_growth_total_hp = add_hp_cycle(real_avg_wage_total_sa_growth, "Real_Wage_Growth_SA_Annual")

count_growth_by_sector_hp = add_hp_cycle(count_sector_sa_growth, "Annual_Emp_Growth")
count_growth_total_hp = add_hp_cycle(count_total_sa_growth, "Annual_Emp_Growth")

# --- Volatilidades de crecimientos: calcular y guardar por sector y total ---
print("\n[INFO] Calculando volatilidades de crecimiento (std de tasas mensuales)...")

# Empleo crecimiento: por sector y total
vol_emp_growth_sector_sa = count_sector_sa_growth.groupby("MIP_sector_id")["Monthly_Emp_Growth"].std().reset_index().rename(columns={"Monthly_Emp_Growth": "Vol_Emp_Growth_SA"})
vol_emp_growth_total_sa = count_total_sa_growth["Monthly_Emp_Growth"].std()

# Nominal wage growth: por sector y total
vol_nominal_wage_growth_sector_sa = avg_wage_by_sector_sa_growth.groupby("MIP_sector_id")["Nominal_Wage_Growth_SA"].std().reset_index().rename(columns={"Nominal_Wage_Growth_SA": "Vol_Nominal_Wage_Growth_SA"})
vol_nominal_wage_growth_total_sa = avg_wage_total_sa_growth["Nominal_Wage_Growth_SA"].std()

# Real wage growth: por sector y total
vol_real_wage_growth_sector_sa = real_avg_wage_by_sector_sa_growth.groupby("MIP_sector_id")["Real_Wage_Growth_SA"].std().reset_index().rename(columns={"Real_Wage_Growth_SA": "Vol_Real_Wage_Growth_SA"})
vol_real_wage_growth_total_sa = real_avg_wage_total_sa_growth["Real_Wage_Growth_SA"].std()


import os
import pandas as pd
import numpy as np

base_path = '/Users/valentinavasquez/Documents/GitHub/Public_AFC/output/Industrial_Sectors'

# 1. Conteo de trabajadores
conteo_path = os.path.join(base_path, '1_conteo_trabajadores')
os.makedirs(conteo_path, exist_ok=True)
count_by_sector.toPandas().to_csv(os.path.join(conteo_path, 'count_by_sector.csv'), index=False)
count_total.toPandas().to_csv(os.path.join(conteo_path, 'count_total.csv'), index=False)
agg_count_by_sector.toPandas().to_csv(os.path.join(conteo_path, 'agg_count_by_sector.csv'), index=False)
pd.DataFrame({'agg_count_total': [agg_count_total]}).to_csv(os.path.join(conteo_path, 'agg_count_total.csv'), index=False)    

# 2. Shares de trabajadores
shares_path = os.path.join(base_path, '2_shares_trabajadores')
os.makedirs(shares_path, exist_ok=True)
shares_by_sector.toPandas().to_csv(os.path.join(shares_path, 'shares_by_sector.csv'), index=False)
agg_shares_by_sector.toPandas().to_csv(os.path.join(shares_path, 'agg_shares_by_sector.csv'), index=False)

# 3. Salarios nominales
nominal_path = os.path.join(base_path, '3_salarios_nominales')
os.makedirs(nominal_path, exist_ok=True)
avg_wage_by_sector.toPandas().to_csv(os.path.join(nominal_path, 'avg_wage_by_sector.csv'), index=False)
avg_wage_total.toPandas().to_csv(os.path.join(nominal_path, 'avg_wage_total.csv'), index=False)
agg_avg_wage_by_sector.toPandas().to_csv(os.path.join(nominal_path, 'agg_avg_wage_by_sector.csv'), index=False)
pd.DataFrame({'agg_avg_wage_total': [agg_avg_wage_total]}).to_csv(os.path.join(nominal_path, 'agg_avg_wage_total.csv'), index=False)

# 4. Salarios reales
real_path = os.path.join(base_path, '4_salarios_reales')
os.makedirs(real_path, exist_ok=True)
real_avg_wage_by_sector.toPandas().to_csv(os.path.join(real_path, 'real_avg_wage_by_sector.csv'), index=False)
real_avg_wage_total.toPandas().to_csv(os.path.join(real_path, 'real_avg_wage_total.csv'), index=False)
agg_real_avg_wage_by_sector.toPandas().to_csv(os.path.join(real_path, 'agg_real_avg_wage_by_sector.csv'), index=False)
pd.DataFrame({'agg_real_avg_wage_total': [agg_real_avg_wage_total]}).to_csv(os.path.join(real_path, 'agg_real_avg_wage_total.csv'), index=False)

# 5. Series desestacionalizadas 
sa_path = os.path.join(base_path, '5_desestacionalizadas')
os.makedirs(sa_path, exist_ok=True)
count_total_sa.to_csv(os.path.join(sa_path, 'count_total_sa.csv'), index=False)
count_by_sector_sa.to_csv(os.path.join(sa_path, 'count_by_sector_sa.csv'), index=False)
avg_wage_by_sector_sa.to_csv(os.path.join(sa_path, 'avg_wage_by_sector_sa.csv'), index=False)
avg_wage_total_sa.to_csv(os.path.join(sa_path, 'avg_wage_total_sa.csv'), index=False)
real_avg_wage_by_sector_sa.to_csv(os.path.join(sa_path, 'real_avg_wage_by_sector_sa.csv'), index=False)
real_avg_wage_total_sa.to_csv(os.path.join(sa_path, 'real_avg_wage_total_sa.csv'), index=False)

# 5b. Promedios móviles de 3 meses
ma3_path = os.path.join(base_path, '5b_promedios_moviles_3m')
os.makedirs(ma3_path, exist_ok=True)
pdf_nominal_sector_ma3.to_csv(os.path.join(ma3_path, 'avg_wage_by_sector_ma3.csv'), index=False)
avg_wage_total_ma3.to_csv(os.path.join(ma3_path, 'avg_wage_total_ma3.csv'), index=False)
pdf_real_sector_ma3.to_csv(os.path.join(ma3_path, 'real_avg_wage_by_sector_ma3.csv'), index=False)
real_avg_wage_total_ma3.to_csv(os.path.join(ma3_path, 'real_avg_wage_total_ma3.csv'), index=False)
avg_wage_by_sector_ma3_sa.to_csv(os.path.join(ma3_path, 'avg_wage_by_sector_ma3_sa.csv'), index=False)
avg_wage_total_ma3_sa.to_csv(os.path.join(ma3_path, 'avg_wage_total_ma3_sa.csv'), index=False)
real_avg_wage_by_sector_ma3_sa.to_csv(os.path.join(ma3_path, 'real_avg_wage_by_sector_ma3_sa.csv'), index=False)
real_avg_wage_total_ma3_sa.to_csv(os.path.join(ma3_path, 'real_avg_wage_total_ma3_sa.csv'), index=False)

# 6. Crecimientos (mensual y anual)
growth_path = os.path.join(base_path, '6_crecimientos')
os.makedirs(growth_path, exist_ok=True)
count_sector_sa_growth.to_csv(os.path.join(growth_path, 'count_sector_sa_growth.csv'), index=False)
count_total_sa_growth.to_csv(os.path.join(growth_path, 'count_total_sa_growth.csv'), index=False)
avg_wage_by_sector_sa_growth.to_csv(os.path.join(growth_path, 'avg_wage_by_sector_sa_growth.csv'), index=False)
avg_wage_total_sa_growth.to_csv(os.path.join(growth_path, 'avg_wage_total_sa_growth.csv'), index=False)
real_avg_wage_by_sector_sa_growth.to_csv(os.path.join(growth_path, 'real_avg_wage_by_sector_sa_growth.csv'), index=False)
real_avg_wage_total_sa_growth.to_csv(os.path.join(growth_path, 'real_avg_wage_total_sa_growth.csv'), index=False)
avg_wage_by_sector_ma3_sa_growth.to_csv(os.path.join(growth_path, 'avg_wage_by_sector_ma3_sa_growth.csv'),index=False)
avg_wage_total_ma3_sa_growth.to_csv(os.path.join(growth_path, 'avg_wage_total_ma3_sa_growth.csv'),index=False)
real_avg_wage_by_sector_ma3_sa_growth.to_csv(os.path.join(growth_path, 'real_avg_wage_by_sector_ma3_sa_growth.csv'),index=False)
real_avg_wage_total_ma3_sa_growth.to_csv(os.path.join(growth_path, 'real_avg_wage_total_ma3_sa_growth.csv'),index=False)

# 7. Volatilidades
vol_path = os.path.join(base_path, '7_volatilidades')
os.makedirs(vol_path, exist_ok=True)
vol_nominal_wage_sector_sa.to_csv(os.path.join(vol_path, 'vol_nominal_wage_sector_sa.csv'), index=False)
vol_real_wage_sector_sa.to_csv(os.path.join(vol_path, 'vol_real_wage_sector_sa.csv'), index=False)
vol_emp_sector_sa.to_csv(os.path.join(vol_path, 'vol_emp_sector_sa.csv'), index=False)
pd.DataFrame({'Vol_Nominal_Wage_Total_SA': [np.nanstd(avg_wage_total_sa["Avg_Taxable_Income_sa"])]}).to_csv(os.path.join(vol_path, 'vol_nominal_wage_total_sa.csv'), index=False)
pd.DataFrame({'Vol_Real_Wage_Total_SA': [np.nanstd(real_avg_wage_total_sa["Real_Avg_Wage_sa"])]}).to_csv(os.path.join(vol_path, 'vol_real_wage_total_sa.csv'), index=False)

# Guardar volatilidades de crecimiento
vol_emp_growth_sector_sa.to_csv(os.path.join(vol_path, 'vol_emp_growth_sector_sa.csv'), index=False)
pd.DataFrame({'Vol_Emp_Growth_Total_SA': [vol_emp_growth_total_sa]}).to_csv(os.path.join(vol_path, 'vol_emp_growth_total_sa.csv'), index=False)

vol_nominal_wage_growth_sector_sa.to_csv(os.path.join(vol_path, 'vol_nominal_wage_growth_sector_sa.csv'), index=False)
pd.DataFrame({'Vol_Nominal_Wage_Growth_Total_SA': [vol_nominal_wage_growth_total_sa]}).to_csv(os.path.join(vol_path, 'vol_nominal_wage_growth_total_sa.csv'), index=False)

vol_real_wage_growth_sector_sa.to_csv(os.path.join(vol_path, 'vol_real_wage_growth_sector_sa.csv'), index=False)
pd.DataFrame({'Vol_Real_Wage_Growth_Total_SA': [vol_real_wage_growth_total_sa]}).to_csv(os.path.join(vol_path, 'vol_real_wage_growth_total_sa.csv'), index=False)
pd.DataFrame({'Vol_Emp_Total_SA': [np.nanstd(count_total_sa["Count_Workers_sa"])]}).to_csv(os.path.join(vol_path, 'vol_emp_total_sa.csv'), index=False)

# 8. HP filter
hp_path = os.path.join(base_path, '8_hp_filter')
os.makedirs(hp_path, exist_ok=True)
avg_wage_by_sector_sa_hp.to_csv(os.path.join(hp_path, 'avg_wage_by_sector_sa_hp_cycle.csv'), index=False)
avg_wage_total_sa_hp.to_csv(os.path.join(hp_path, 'avg_wage_total_sa_hp_cycle.csv'), index=False)
real_avg_wage_by_sector_sa_hp.to_csv(os.path.join(hp_path, 'real_avg_wage_by_sector_sa_hp_cycle.csv'), index=False)
real_avg_wage_total_sa_hp.to_csv(os.path.join(hp_path, 'real_avg_wage_total_sa_hp_cycle.csv'), index=False)
count_by_sector_sa_hp.to_csv(os.path.join(hp_path, 'count_by_sector_sa_hp_cycle.csv'), index=False)
count_total_sa_hp.to_csv(os.path.join(hp_path, 'count_total_sa_hp_cycle.csv'), index=False)

# Guardar HP cycles de crecimientos anuales
nominal_growth_by_sector_hp.to_csv(os.path.join(hp_path, 'avg_wage_by_sector_sa_growth_annual_hp_cycle.csv'), index=False)
nominal_growth_total_hp.to_csv(os.path.join(hp_path, 'avg_wage_total_sa_growth_annual_hp_cycle.csv'), index=False)

real_growth_by_sector_hp.to_csv(os.path.join(hp_path, 'real_avg_wage_by_sector_sa_growth_annual_hp_cycle.csv'), index=False)
real_growth_total_hp.to_csv(os.path.join(hp_path, 'real_avg_wage_total_sa_growth_annual_hp_cycle.csv'), index=False)

count_growth_by_sector_hp.to_csv(os.path.join(hp_path, 'count_by_sector_sa_growth_annual_hp_cycle.csv'), index=False)
count_growth_total_hp.to_csv(os.path.join(hp_path, 'count_total_sa_growth_annual_hp_cycle.csv'), index=False)


