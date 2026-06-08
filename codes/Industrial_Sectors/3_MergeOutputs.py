import os
import re
import warnings
import unicodedata
from functools import reduce

import pandas as pd


# ============================================================
# 1. PATHS
# ============================================================

try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
except NameError:
    project_root = os.path.abspath("../../")

base = os.path.join(project_root, "output", "Industrial_Sectors")
bases_dir = os.path.join(project_root, "bases")
imacec_file = os.path.join(
    project_root,
    "bases",
    "IMACEC",
    "IMACEC_2008_2018_combined_base2018_12sectors.csv"
)

os.makedirs(bases_dir, exist_ok=True)


# ============================================================
# 2. HELPERS
# ============================================================

def read_keep(relative_path, cols):
    """
    Read CSV from Industrial_Sectors and keep only selected columns.
    """
    path = os.path.join(base, relative_path)
    df = pd.read_csv(path)

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en {path}: {missing}")

    return df[cols].copy()


def sanitize_name(s):
    """
    Normalize sector names for matching.
    """
    if pd.isna(s):
        return ""

    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[\s\-/.,()]+", "_", s)
    s = re.sub(r"[^0-9a-zA-Z_]", "", s)
    s = re.sub(r"__+", "_", s)

    return s.strip("_")


def merge_dataframes(dfs, keys):
    """
    Sequential left merge using the first dataframe as base.
    """
    return reduce(
        lambda left, right: left.merge(right, on=keys, how="left"),
        dfs
    )


# ============================================================
# 3. LOAD MAIN DATASETS
# ============================================================

df_count = read_keep(
    "1_conteo_trabajadores/count_by_sector.csv",
    ["Wage_Date", "MIP_sector_id", "MIP_sector_name", "Count_Workers"]
)

df_shares = read_keep(
    "2_shares_trabajadores/shares_by_sector.csv",
    ["Wage_Date", "MIP_sector_id", "Share"]
)

df_nom_wage = read_keep(
    "3_salarios_nominales/avg_wage_by_sector.csv",
    ["Wage_Date", "MIP_sector_id", "Avg_Taxable_Income"]
)

df_real_wage = read_keep(
    "4_salarios_reales/real_avg_wage_by_sector.csv",
    ["Wage_Date", "MIP_sector_id", "Real_Avg_Wage"]
)

df_sa_wage = read_keep(
    "5_desestacionalizadas/avg_wage_by_sector_sa.csv",
    ["Wage_Date", "MIP_sector_id", "Avg_Taxable_Income_sa"]
)

df_sa_real_wage = read_keep(
    "5_desestacionalizadas/real_avg_wage_by_sector_sa.csv",
    ["Wage_Date", "MIP_sector_id", "Real_Avg_Wage_sa"]
)

df_sa_count = read_keep(
    "5_desestacionalizadas/count_by_sector_sa.csv",
    ["Wage_Date", "MIP_sector_id", "Count_Workers_sa"]
)

df_growth = read_keep(
    "6_crecimientos/avg_wage_by_sector_sa_growth.csv",
    [
        "Wage_Date",
        "MIP_sector_id",
        "Nominal_Wage_Growth_SA",
        "Nominal_Wage_Growth_SA_Annual"
    ]
)

df_growth_real = read_keep(
    "6_crecimientos/real_avg_wage_by_sector_sa_growth.csv",
    [
        "Wage_Date",
        "MIP_sector_id",
        "Real_Wage_Growth_SA",
        "Real_Wage_Growth_SA_Annual"
    ]
)

df_growth_count = read_keep(
    "6_crecimientos/count_sector_sa_growth.csv",
    [
        "Wage_Date",
        "MIP_sector_id",
        "Monthly_Emp_Growth",
        "Annual_Emp_Growth"
    ]
)

df_hp_nominal_cycle = read_keep(
    "8_hp_filter/avg_wage_by_sector_sa_hp_cycle.csv",
    ["Wage_Date", "MIP_sector_id", "Avg_Taxable_Income_sa_cycle"]
)

df_hp_real_cycle = read_keep(
    "8_hp_filter/real_avg_wage_by_sector_sa_hp_cycle.csv",
    ["Wage_Date", "MIP_sector_id", "Real_Avg_Wage_sa_cycle"]
)

df_hp_count_cycle = read_keep(
    "8_hp_filter/count_by_sector_sa_hp_cycle.csv",
    ["Wage_Date", "MIP_sector_id", "Count_Workers_sa_cycle"]
)

df_hp_nominal_growth = read_keep(
    "8_hp_filter/avg_wage_by_sector_sa_growth_annual_hp_cycle.csv",
    ["Wage_Date", "MIP_sector_id", "Nominal_Wage_Growth_SA_Annual_cycle"]
)

df_hp_real_growth = read_keep(
    "8_hp_filter/real_avg_wage_total_sa_growth_annual_hp_cycle.csv",
    ["Wage_Date", "MIP_sector_id", "Real_Wage_Growth_SA_Annual_cycle"]
)

df_hp_count_growth = read_keep(
    "8_hp_filter/count_by_sector_sa_growth_annual_hp_cycle.csv",
    ["Wage_Date", "MIP_sector_id", "Annual_Emp_Growth_cycle"]
)


# ============================================================
# 4. MERGE MAIN DATASETS
# ============================================================

dfs_to_merge = [
    df_count,
    df_shares,
    df_nom_wage,
    df_real_wage,
    df_sa_wage,
    df_sa_real_wage,
    df_sa_count,
    df_growth,
    df_growth_real,
    df_growth_count,
    df_hp_nominal_cycle,
    df_hp_real_cycle,
    df_hp_count_cycle,
    df_hp_nominal_growth,
    df_hp_real_growth,
    df_hp_count_growth,
]

df = merge_dataframes(
    dfs=dfs_to_merge,
    keys=["Wage_Date", "MIP_sector_id"]
)

df["Wage_Date"] = pd.to_datetime(df["Wage_Date"])


# ============================================================
# 5. MERGE IMACEC 12-SECTOR DATA
# ============================================================

IMACEC_TO_MIP = {
    "Agriculture, forestry and fishing": "MIP_1_Agropecuario_y_Pesca",
    "Business services": "MIP_10_Servicios_Empresariales",
    "Construction": "MIP_5_Construccion",
    "Electricity, gas, water and waste management": "MIP_4_Electricidad_gas_agua",
    "Financial intermediation": "MIP_8_Servicios_Financieros",
    "Manufacturing": "MIP_3_Industria_Manufacturera",
    "Mining": "MIP_2_Mineria",
    "Personal services": "MIP_11_Servicios_Personales",
    "Public administration": "MIP_12_Administracion_Publica",
    "Real estate and housing services": "MIP_9_Vivienda",
    "Transport, communications and information services": "MIP_7_Transporte_Comunicaciones",
    "Wholesale and retail trade, accommodation and food services": "MIP_6_Comercio_y_Restaurantes",
}

if os.path.exists(imacec_file):
    df_im = pd.read_csv(imacec_file)

    if "date" not in df_im.columns:
        raise ValueError(f"No encontré columna 'date' en {imacec_file}")

    df_im["Wage_Date"] = pd.to_datetime(df_im["date"], errors="coerce")

    value_cols = [
        c for c in df_im.columns
        if c not in ["date", "Wage_Date"]
    ]

    if not value_cols:
        warnings.warn(f"No encontré columnas sectoriales en {imacec_file}")

    else:
        im_long = df_im[["Wage_Date"] + value_cols].melt(
            id_vars="Wage_Date",
            var_name="imacec_sector",
            value_name="IMACEC_12sector"
        )

        # Mapping robusto: acepta tanto nombres largos como códigos MIP_...
        reverse_label = {v: k for k, v in IMACEC_TO_MIP.items()}

        mapping_sanitized = {
            sanitize_name(k): k
            for k in IMACEC_TO_MIP.keys()
        }

        mapping_sanitized.update({
            sanitize_name(v): reverse_label[v]
            for v in IMACEC_TO_MIP.values()
        })

        im_long["imacec_s"] = im_long["imacec_sector"].apply(sanitize_name)
        im_long["MIP_sector_name"] = im_long["imacec_s"].map(mapping_sanitized)

        missing_after = (
            im_long.loc[im_long["MIP_sector_name"].isna(), "imacec_sector"]
            .drop_duplicates()
            .tolist()
        )

        if missing_after:
            warnings.warn(
                "Estas columnas IMACEC no pudieron mapearse a sectores MIP "
                f"y serán omitidas: {missing_after}"
            )

        im_long = im_long.dropna(subset=["MIP_sector_name"])

        df = df.merge(
            im_long[["Wage_Date", "MIP_sector_name", "IMACEC_12sector"]],
            on=["Wage_Date", "MIP_sector_name"],
            how="left"
        )

        print(f"IMACEC 12-sector merged from: {imacec_file}")

else:
    warnings.warn(
        f"No encontré el archivo IMACEC en {imacec_file}. "
        "Se omite merge con IMACEC."
    )


# ============================================================
# 6. SAVE FINAL DATASET
# ============================================================

df = df.sort_values(["MIP_sector_id", "Wage_Date"]).reset_index(drop=True)

out_path = os.path.join(
    bases_dir,
    "consolidated_by_sector_with_IMACEC.csv"
)

df.to_csv(out_path, index=False, encoding="utf-8")

print("\nFinal merged df shape:", df.shape)
print("Final merged dataframe saved to:", out_path)