# keep column names safe for CSV (remove accents / odd chars)

import os
import re
import unicodedata
import pandas as pd


# ============================================================
# 1. PATHS
# ============================================================

try:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
except NameError:
    base_dir = os.path.abspath("../../")

imacec_dir = os.path.join(base_dir, "bases", "IMACEC")

file_2008 = os.path.join(imacec_dir, "IMACEC_2008.xlsx")
file_2018 = os.path.join(imacec_dir, "IMACEC_2018.xlsx")

out_combined = os.path.join(
    imacec_dir,
    "IMACEC_2008_2018_combined_base2018.csv"
)

out_agg = os.path.join(
    imacec_dir,
    "IMACEC_2008_2018_combined_base2018_12sectors.csv"
)

out_pib = os.path.join(
    imacec_dir,
    "IMACEC_PIB_base2018.csv"
)

os.makedirs(imacec_dir, exist_ok=True)


# ============================================================
# 2. HELPERS
# ============================================================

def sanitize_col(name):
    """Remove accents and problematic characters from column names."""
    if not isinstance(name, str):
        return name

    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.replace(".", "")
    s = s.replace("/", "_")
    s = s.replace(" ", "_")
    s = re.sub(r"[^0-9A-Za-z_\-]", "", s)

    return s.strip("_")


def read_imacec(path):
    """
    Read IMACEC Excel file and return a DataFrame with:
    - date column
    - all numeric series columns

    Requires a column named 'Periodo'.
    """
    df = pd.read_excel(path, header=2)

    if "Periodo" not in df.columns:
        raise ValueError(
            f"No pude detectar columna 'Periodo' en {path}. "
            f"Columnas encontradas: {df.columns.tolist()}"
        )

    date_col = "Periodo"

    value_cols = [
        c for c in df.columns
        if c != date_col and pd.api.types.is_numeric_dtype(df[c])
    ]

    if not value_cols:
        raise ValueError(
            f"No pude detectar columnas numéricas en {path}. "
            f"Columnas encontradas: {df.columns.tolist()}"
        )

    out = pd.DataFrame({
        "date": pd.to_datetime(df[date_col])
    })

    for c in value_cols:
        out[c] = pd.to_numeric(df[c], errors="coerce")

    out = out.sort_values("date").reset_index(drop=True)

    return out


def export_pib_series(combined, cols_map, out_file):
    """Export PIB series if a PIB column is detected."""

    pib_orig = None

    for c in cols_map.keys():
        clean_name = re.sub(r"\s+", "", str(c)).lower()
        if clean_name == "pib":
            pib_orig = c
            break

    if pib_orig is None:
        for c in cols_map.keys():
            if "pib" in str(c).lower():
                pib_orig = c
                break

    if pib_orig is None:
        print("No encontré una columna llamada PIB. No exporto serie PIB.")
        return

    pib_col_safe = cols_map.get(pib_orig)

    if pib_col_safe not in combined.columns:
        print(
            f"Columna detectada como PIB ({pib_col_safe}) no está en combined. "
            "No exporto serie PIB."
        )
        return

    combined[["date", pib_col_safe]].to_csv(
        out_file,
        index=False,
        encoding="utf-8"
    )

    print(f"Serie PIB exportada en: {out_file}")


# ============================================================
# 3. READ DATA
# ============================================================

series_2008 = read_imacec(file_2008)
series_2018 = read_imacec(file_2018)

series_2008["year"] = series_2008["date"].dt.year
series_2018["year"] = series_2018["date"].dt.year


# ============================================================
# 4. TARGET YEAR FOR LINKING FACTORS
# ============================================================

years_2008 = set(series_2008["year"].dropna().astype(int).unique())
years_2018 = set(series_2018["year"].dropna().astype(int).unique())

common_years = sorted(years_2008 & years_2018)

if not common_years:
    raise ValueError(
        "No hay años en común entre ambas series. "
        "No puedo calcular el factor de conversión."
    )

target_year = 2018 if 2018 in common_years else max(common_years)

if target_year != 2018:
    print(
        f"Advertencia: 2018 no está en ambas series. "
        f"Usando año {target_year} para calcular factores."
    )


# ============================================================
# 5. MATCH COLUMNS AND COMPUTE SCALE FACTORS
# ============================================================

cols_2008 = [c for c in series_2008.columns if c not in ["date", "year"]]
cols_2018 = [c for c in series_2018.columns if c not in ["date", "year"]]

if not cols_2008 or not cols_2018:
    raise ValueError(
        "No se detectaron columnas numéricas en alguno de los archivos IMACEC."
    )

pair_count = min(len(cols_2008), len(cols_2018))
pairs = [(cols_2008[i], cols_2018[i]) for i in range(pair_count)]

print("\nPares usados para empalmar:")
for c2008, c2018 in pairs:
    print(f"  {c2008}  -->  {c2018}")

scales = {}

for col2008, col2018 in pairs:
    mean_2008 = series_2008.loc[
        series_2008["year"] == target_year,
        col2008
    ].mean()

    mean_2018 = series_2018.loc[
        series_2018["year"] == target_year,
        col2018
    ].mean()

    if pd.isna(mean_2008) or pd.isna(mean_2018) or mean_2008 == 0:
        scales[col2018] = None
    else:
        scales[col2018] = mean_2018 / mean_2008


# ============================================================
# 6. CONVERT 2008 SERIES TO BASE 2018
# ============================================================

series_2008_base2018 = series_2008[["date"]].copy()

for col2008, col2018 in pairs:
    scale = scales.get(col2018)

    if scale is None:
        series_2008_base2018[col2018] = pd.NA
    else:
        series_2008_base2018[col2018] = series_2008[col2008] * scale

if len(cols_2018) > pair_count:
    for extra in cols_2018[pair_count:]:
        series_2008_base2018[extra] = pd.NA


# ============================================================
# 7. COMBINE SERIES
# ============================================================

series_2018_idx = series_2018.set_index("date")
series_2008_idx = series_2008_base2018.set_index("date")

full_index = series_2008_idx.index.union(series_2018_idx.index).sort_values()

combined = pd.DataFrame(index=full_index)

for col in cols_2018:
    series_new = (
        series_2018_idx[col]
        if col in series_2018_idx.columns
        else pd.Series(index=full_index, dtype=float)
    )

    series_old = (
        series_2008_idx[col]
        if col in series_2008_idx.columns
        else pd.Series(index=full_index, dtype=float)
    )

    combined[col] = (
        series_new.reindex(full_index)
        .combine_first(series_old.reindex(full_index))
    )

combined = combined.reset_index().rename(columns={"index": "date"})


# ============================================================
# 8. SANITIZE COLUMN NAMES
# ============================================================

cols_map = {
    c: sanitize_col(c)
    for c in combined.columns
    if c != "date"
}

combined = combined.rename(columns=cols_map)


# ============================================================
# 9. SAVE COMBINED SERIES
# ============================================================

combined.to_csv(out_combined, index=False, encoding="utf-8")

print(f"\nSerie combinada guardada en: {out_combined}")
print(
    "Rango de fechas:",
    combined["date"].min(),
    "→",
    combined["date"].max()
)

print("\nFactores aplicados:")
for orig_name, factor in scales.items():
    safe_name = sanitize_col(orig_name)
    print(f"  {safe_name}: {factor}")


# ============================================================
# 10. AGGREGATE TO 12 MIP SECTORS
# ============================================================

agg_map = {
    "MIP_1_Agropecuario_y_Pesca": [
        "1_Agropecuario-silvicola",
        "2_Pesca"
    ],
    "MIP_2_Mineria": [
        "3_Mineria"
    ],
    "MIP_3_Industria_Manufacturera": [
        "4_Industria_Manufacturera"
    ],
    "MIP_4_Electricidad_gas_agua": [
        "6_Electricidad_gas_agua_y_gestion_de_desechos"
    ],
    "MIP_5_Construccion": [
        "7_Construccion"
    ],
    "MIP_6_Comercio_y_Restaurantes": [
        "8_Comercio",
        "9_Restaurantes_y_hoteles"
    ],
    "MIP_7_Transporte_Comunicaciones": [
        "10_Transporte",
        "11_Comunicaciones_y_servicios_de_informacion"
    ],
    "MIP_8_Servicios_Financieros": [
        "12_Servicios_financieros"
    ],
    "MIP_9_Vivienda": [
        "14_Servicios_de_vivienda_e_inmobiliarios"
    ],
    "MIP_10_Servicios_Empresariales": [
        "13_Servicios_empresariales"
    ],
    "MIP_11_Servicios_Personales": [
        "15_Servicios_personales"
    ],
    "MIP_12_Administracion_Publica": [
        "16_Administracion_publica"
    ],
}

agg_map = {
    aggnm: [sanitize_col(c) for c in cols]
    for aggnm, cols in agg_map.items()
}

agg_df = combined[["date"]].copy()
missing_cols = {}

for aggnm, cols in agg_map.items():
    present = [c for c in cols if c in combined.columns]
    missing = [c for c in cols if c not in combined.columns]

    if missing:
        missing_cols[aggnm] = missing

    if present:
        agg_df[aggnm] = combined[present].sum(
            axis=1,
            skipna=True,
            min_count=1
        )
    else:
        agg_df[aggnm] = pd.NA

agg_df.to_csv(out_agg, index=False, encoding="utf-8")

print(f"\nArchivo agregado a 12 sectores guardado en: {out_agg}")

if missing_cols:
    print("\nAdvertencia: algunas columnas del mapa de agregación no estaban presentes:")
    for sector, cols in missing_cols.items():
        print(f"  {sector}: faltan {cols}")


# ============================================================
# 11. EXPORT PIB SERIES
# ============================================================

export_pib_series(
    combined=combined,
    cols_map=cols_map,
    out_file=out_pib
)



