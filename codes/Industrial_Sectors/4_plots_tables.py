import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np

# Base output folder (relative to this script)
base = os.path.join(os.path.dirname(__file__), '../../output/Industrial_Sectors/')
# Prefer merged file in `bases/` (user requested), else fall back to output/9_merged
bases_dir = os.path.join(os.path.dirname(__file__), '../../bases')
merged_bases = os.path.join(bases_dir, 'consolidated_by_sector_with_IMACEC.csv')
merged_dir = os.path.join(base, '9_merged')
merged_output = os.path.join(merged_dir, 'consolidated_by_sector_with_IMACEC.csv')

if os.path.exists(merged_bases):
    merged_file = merged_bases
elif os.path.exists(merged_output):
    merged_file = merged_output
else:
    raise FileNotFoundError(f'Merged file not found in bases or output paths. Checked: {merged_bases} and {merged_output}')

# Read merged dataframe
df = pd.read_csv(merged_file, parse_dates=['Wage_Date'])

plots_dir = os.path.join(base, 'Plots')
os.makedirs(plots_dir, exist_ok=True)
# Directory for exported tables (CSV / TeX)
tables_dir = os.path.join(base, 'tables')
os.makedirs(tables_dir, exist_ok=True)


# Helper: guardar DataFrame como tabla LaTeX con valores coloreados
def save_df_as_colored_tex(df, value_col, out_path, caption=None):
    df2 = df.copy()
    # Remove observation counts column if present; not needed in exported table
    if 'n_obs' in df2.columns:
        df2 = df2.drop(columns=['n_obs'])
    def fmt(v):
        if pd.isna(v):
            return '--'
        try:
            val = float(v)
        except Exception:
            return str(v)
        # Positive values: plain (black). Negative values: red.
        if val < 0:
            return f"\\textcolor{{red}}{{{val:.3f}}}"
        else:
            return f"{val:.3f}"
    if value_col in df2.columns:
        df2[value_col] = df2[value_col].apply(fmt)
    header = '% Requires \\usepackage{xcolor}\n'
    # Try pandas to_latex (may require Jinja2). If that fails, write a manual longtable
    try:
        tex = df2.to_latex(index=False, escape=False)
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(header)
            if caption:
                f.write(f'% {caption}\n')
            f.write(tex)
        print(f'Saved LaTeX table: {out_path}')
        return
    except Exception:
        # Manual writer: preserve LaTeX commands in formatted cells (e.g. \\textcolor)
        def escape_header(s):
            s = str(s)
            for a, b in [('&', '\\&'), ('%', '\\%'), ('$', '\\$'), ('#', '\\#'), ('_', '\\_'), ('{', '\\{'), ('}', '\\}'), ('~', '\\textasciitilde{}'), ('^', '\\textasciicircum{}')]:
                s = s.replace(a, b)
            s = s.replace('\\', '\\textbackslash{}')
            return s

        def escape_cell(v):
            if pd.isna(v):
                return '--'
            s = str(v)
            # If the cell contains a backslash, assume it contains LaTeX commands and
            # avoid escaping braces/backslashes so commands like \\textcolor{} stay intact.
            if '\\' in s:
                for a, b in [('&', '\\&'), ('%', '\\%'), ('$', '\\$'), ('#', '\\#'), ('_', '\\_'), ('~', '\\textasciitilde{}'), ('^', '\\textasciicircum{}')]:
                    s = s.replace(a, b)
                return s
            # otherwise escape common LaTeX specials including braces
            for a, b in [('\\', '\\textbackslash{}'), ('&', '\\&'), ('%', '\\%'), ('$', '\\$'), ('#', '\\#'), ('_', '\\_'), ('{', '\\{'), ('}', '\\}'), ('~', '\\textasciitilde{}'), ('^', '\\textasciicircum{}')]:
                s = s.replace(a, b)
            return s

        cols = list(df2.columns)
        ncols = len(cols)
        colspec = 'l' + 'r' * (ncols - 1) if ncols >= 1 else ''
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(header)
            if caption:
                f.write(f'% {caption}\n')
            f.write('\\begin{longtable}{' + colspec + '}\n')
            f.write('\\hline\n')
            header_row = ' & '.join([escape_header(c) for c in cols])
            f.write(header_row + ' \\\\ \n')
            f.write('\\hline\\endfirsthead\n')
            f.write('\\hline\n')
            f.write(header_row + ' \\\\ \n')
            f.write('\\hline\\endhead\n')
            for _, row in df2.iterrows():
                cells = [escape_cell(row[c]) for c in cols]
                f.write(' & '.join(cells) + ' \\\\ \n')
            f.write('\\hline\n')
            f.write('\\end{longtable}\n')
        print(f'Saved LaTeX table: {out_path}')


def save_df_as_tex(df, out_path, caption=None):
    """Save DataFrame as plain LaTeX table (no color highlighting)."""
    # Try pandas to_latex first; fallback to manual longtable writer to avoid Jinja2 requirement
    try:
        tex = df.to_latex(index=False, escape=True)
        with open(out_path, 'w', encoding='utf-8') as f:
            if caption:
                f.write(f'% {caption}\n')
            f.write(tex)
        print(f'Saved LaTeX table (plain): {out_path}')
        return
    except Exception:
        # manual writer
        def escape_latex_str(s):
            if pd.isna(s):
                return ''
            s = str(s)
            s = s.replace('\\', '\\textbackslash{}')
            for a, b in [('&', '\\&'), ('%', '\\%'), ('$', '\\$'), ('#', '\\#'), ('_', '\\_'), ('{', '\\{'), ('}', '\\}'), ('~', '\\textasciitilde{}'), ('^', '\\textasciicircum{}')]:
                s = s.replace(a, b)
            return s

        cols = list(df.columns)
        ncols = len(cols)
        colspec = 'l' + 'r' * (ncols - 1) if ncols >= 1 else ''
        with open(out_path, 'w', encoding='utf-8') as f:
            if caption:
                f.write(f'% {caption}\n')
            f.write('\\begin{longtable}{' + colspec + '}\n')
            f.write('\\hline\n')
            header = ' & '.join([escape_latex_str(c) for c in cols])
            f.write(header + ' \\\\ \n')
            f.write('\\hline\\endfirsthead\n')
            f.write('\\hline\n')
            f.write(header + ' \\\\ \n')
            f.write('\\hline\\endhead\n')
            for _, row in df.iterrows():
                cells = []
                for v in row:
                    if pd.isna(v):
                        cells.append('')
                    elif isinstance(v, (int,)):
                        cells.append(str(v))
                    elif isinstance(v, float):
                        cells.append(f"{v:.3f}")
                    else:
                        cells.append(escape_latex_str(v))
                f.write(' & '.join(cells) + ' \\\\ \n')
            f.write('\\hline\n')
            f.write('\\end{longtable}\n')
        print(f'Saved LaTeX table (plain, fallback): {out_path}')

# Helper to safe-plot a column if it exists
def plot_by_sector(col, title, ylabel, filename, normalize=False, dropna_subset=None):
    if col not in df.columns:
        print(f'Skipping plot {filename}: column {col} not found')
        return
    plt.figure(figsize=(14, 7))
    for sector, data in df.groupby('MIP_sector_name'):
        data = data.sort_values('Wage_Date')
        series = data[col]
        if dropna_subset is not None and data[dropna_subset].dropna().empty:
            continue
        if normalize:
            # normalize by first non-null
            try:
                first = series.dropna().iloc[0]
                series = series / first
            except Exception:
                pass
        plt.plot(data['Wage_Date'], series, label=sector, alpha=0.7)
    plt.title(title)
    plt.xlabel('Fecha')
    plt.ylabel(ylabel)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')
    plt.tight_layout()
    out = os.path.join(plots_dir, filename)
    plt.savefig(out)
    plt.close()
    print(f'Saved plot: {out}')


# Helper to choose an existing column name from alternatives
def choose_col(preferred, alternatives=None):
    if preferred in df.columns:
        return preferred
    if alternatives:
        for a in alternatives:
            if a in df.columns:
                return a
    return None

# Quantity of workers
plot_by_sector('Count_Workers', 'Cantidad de trabajadores en el tiempo por sector', 'Cantidad de trabajadores', 'trabajadores_por_sector.pdf')
plot_by_sector('Count_Workers_sa', 'Cantidad de trabajadores desestacionalizados por sector', 'Cantidad de trabajadores desestacionalizados', 'trabajadores_desestacionalizados_por_sector.pdf')

# Share
plot_by_sector('Share', 'Share de trabajadores en el tiempo por sector', 'Share (%)', 'share_trabajadores_por_sector.pdf')

# Nominal & real wages (normalized)
plot_by_sector('Avg_Taxable_Income', 'Salario nominal por sector (normalizado, base = 1)', 'Salario normalizado', 'salario_nominal_normalizado_por_sector.pdf', normalize=True)
plot_by_sector('Real_Avg_Wage', 'Salario real por sector (normalizado, base = 1)', 'Salario normalizado', 'salario_real_normalizado_por_sector.pdf', normalize=True, dropna_subset='Real_Avg_Wage')
plot_by_sector('Avg_Taxable_Income_sa', 'Salario nominal desestacionalizado por sector (normalizado, base = 1)', 'Salario normalizado', 'salario_nominal_sa_normalizado_por_sector.pdf', normalize=True, dropna_subset='Avg_Taxable_Income_sa')
plot_by_sector('Real_Avg_Wage_sa', 'Salario real desestacionalizado por sector (normalizado, base = 1)', 'Salario normalizado', 'salario_real_sa_normalizado_por_sector.pdf', normalize=True, dropna_subset='Real_Avg_Wage_sa')

# Crecimiento Salarios Nominal y Real ('Monthly_Emp_Growth', 'Annual_Emp_Growth')
plot_by_sector('Nominal_Wage_Growth_SA', 'Crecimiento mensual de salarios por sector', 'Crecimiento (%)', 'crecimiento_mensual_salarios_por_sector.pdf')
plot_by_sector('Nominal_Wage_Growth_SA_Annual', 'Crecimiento anual de salarios por sector', 'Crecimiento (%)', 'crecimiento_anual_salarios_por_sector.pdf')
plot_by_sector('Real_Wage_Growth_SA', 'Crecimiento mensual de salarios reales por sector', 'Crecimiento (%)', 'crecimiento_mensual_salarios_reales_por_sector.pdf')
plot_by_sector('Real_Wage_Growth_SA_Annual', 'Crecimiento anual de salarios reales por sector', 'Crecimiento (%)', 'crecimiento_anual_salarios_reales_por_sector.pdf')

# Crecimiento Count Trabajadores
mw = choose_col('Monthly_Worker_Growth', ['Monthly_Emp_Growth', 'Monthly_Worker_Growth'])
aw = choose_col('Annual_Worker_Growth', ['Annual_Emp_Growth', 'Annual_Worker_Growth'])
if mw:
    plot_by_sector(mw, 'Crecimiento mensual de trabajadores por sector', 'Crecimiento (%)', 'crecimiento_mensual_trabajadores_por_sector.pdf')
else:
    print('Skipping monthly worker growth plot: no matching column found')
if aw:
    plot_by_sector(aw, 'Crecimiento anual de trabajadores por sector', 'Crecimiento (%)', 'crecimiento_anual_trabajadores_por_sector.pdf')
else:
    print('Skipping annual worker growth plot: no matching column found')

print('All requested plots completed.')

# ── Promedios móviles de 3 meses ──────────────────────────────────────────────
ma3_dir = os.path.join(base, '5b_promedios_moviles_3m')

def _merge_ma3_col(df_base, csv_path, col, key_cols=('Wage_Date', 'MIP_sector_name')):
    """Load a MA3 CSV and merge a single column into df_base."""
    if not os.path.exists(csv_path):
        print(f'MA3 file not found, skipping: {csv_path}')
        return df_base
    tmp = pd.read_csv(csv_path, parse_dates=['Wage_Date'])
    # keep only join keys + target column; avoid duplicate columns
    keep = [c for c in key_cols if c in tmp.columns] + [col]
    missing = [c for c in keep if c not in tmp.columns]
    if missing:
        print(f'Columns {missing} not found in {csv_path}; skipping merge')
        return df_base
    tmp = tmp[keep].drop_duplicates()
    merge_on = [c for c in key_cols if c in df_base.columns and c in tmp.columns]
    if col in df_base.columns:
        df_base = df_base.drop(columns=[col])
    return df_base.merge(tmp, on=merge_on, how='left')

# Merge the 4 MA3 columns into df
df = _merge_ma3_col(df, os.path.join(ma3_dir, 'avg_wage_by_sector_ma3.csv'),    'Avg_Taxable_Income_MA3')
df = _merge_ma3_col(df, os.path.join(ma3_dir, 'avg_wage_by_sector_ma3_sa.csv'), 'Avg_Taxable_Income_MA3_sa')
df = _merge_ma3_col(df, os.path.join(ma3_dir, 'real_avg_wage_by_sector_ma3.csv'),    'Real_Avg_Wage_MA3')
df = _merge_ma3_col(df, os.path.join(ma3_dir, 'real_avg_wage_by_sector_ma3_sa.csv'), 'Real_Avg_Wage_MA3_sa')

# 4 plots – same normalized style as existing wage plots
plot_by_sector(
    'Avg_Taxable_Income_MA3',
    'Salario nominal promedio móvil 3 meses por sector (normalizado, base = 1)',
    'Salario normalizado',
    'salario_nominal_ma3_normalizado_por_sector.pdf',
    normalize=True,
    dropna_subset='Avg_Taxable_Income_MA3',
)
plot_by_sector(
    'Avg_Taxable_Income_MA3_sa',
    'Salario nominal MA3 desestacionalizado por sector (normalizado, base = 1)',
    'Salario normalizado',
    'salario_nominal_ma3_sa_normalizado_por_sector.pdf',
    normalize=True,
    dropna_subset='Avg_Taxable_Income_MA3_sa',
)
plot_by_sector(
    'Real_Avg_Wage_MA3',
    'Salario real promedio móvil 3 meses por sector (normalizado, base = 1)',
    'Salario normalizado',
    'salario_real_ma3_normalizado_por_sector.pdf',
    normalize=True,
    dropna_subset='Real_Avg_Wage_MA3',
)
plot_by_sector(
    'Real_Avg_Wage_MA3_sa',
    'Salario real MA3 desestacionalizado por sector (normalizado, base = 1)',
    'Salario normalizado',
    'salario_real_ma3_sa_normalizado_por_sector.pdf',
    normalize=True,
    dropna_subset='Real_Avg_Wage_MA3_sa',
)

print('MA3 plots completed.')

# Correlación IMACEC vs Empleo por sector
def compute_imacec_employment_correlation(df, imacec_col='IMACEC_12sector', emp_col='Count_Workers', min_periods=6):
    if imacec_col not in df.columns or emp_col not in df.columns:
        print(f'Skipping correlation: missing column {imacec_col} or {emp_col}')
        return
    results = []
    for sector, data in df.groupby('MIP_sector_name'):
        sub = data.sort_values('Wage_Date')[[imacec_col, emp_col]].dropna()
        n = len(sub)
        if n < min_periods:
            corr = float('nan')
        else:
            corr = sub[imacec_col].corr(sub[emp_col])
        results.append({'MIP_sector_name': sector, 'n_obs': n, 'correlation': corr})
    res_df = pd.DataFrame(results).sort_values('correlation', ascending=False)
    out_csv = os.path.join(tables_dir, 'imacec_count_correlation_by_sector.csv')
    res_df.to_csv(out_csv, index=False)
    print(f'Saved correlations CSV: {out_csv}')
    # Exportar .tex coloreado
    out_tex = os.path.join(tables_dir, 'imacec_count_correlation_by_sector.tex')
    save_df_as_colored_tex(res_df, 'correlation', out_tex, caption='Correlación IMACEC-Empleo por sector')


def compute_imacec_wage_correlation(df, imacec_col='IMACEC_12sector', wage_col='Avg_Taxable_Income', min_periods=6):
    """Compute correlation between IMACEC sector series and a wage series by sector.

    Exports CSV and colored .tex ready for LaTeX.
    """
    if imacec_col not in df.columns or wage_col not in df.columns:
        print(f'Skipping correlation: missing column {imacec_col} or {wage_col}')
        return
    results = []
    for sector, data in df.groupby('MIP_sector_name'):
        sub = data.sort_values('Wage_Date')[[imacec_col, wage_col]].dropna()
        n = len(sub)
        if n < min_periods:
            corr = float('nan')
        else:
            corr = sub[imacec_col].corr(sub[wage_col])
        results.append({'MIP_sector_name': sector, 'n_obs': n, 'correlation': corr})
    res_df = pd.DataFrame(results).sort_values('correlation', ascending=False)
    base_name = 'imacec_' + (wage_col.lower()) + '_correlation_by_sector'
    out_csv = os.path.join(tables_dir, f'{base_name}.csv')
    out_tex = os.path.join(tables_dir, f'{base_name}.tex')
    res_df.to_csv(out_csv, index=False)
    save_df_as_colored_tex(res_df, 'correlation', out_tex, caption=f'Correlación IMACEC vs {wage_col} por sector')
    print(f'Saved IMACEC–wage correlation CSV: {out_csv} and TEX: {out_tex}')


compute_imacec_employment_correlation(df)

# Correlación IMACEC vs Salarios (nominal y real)
compute_imacec_wage_correlation(df, wage_col='Avg_Taxable_Income')
compute_imacec_wage_correlation(df, wage_col='Real_Avg_Wage')


# Correlaciones de cada IMACEC sectorial y empleo sectorial con PIB
def compute_correlations_vs_pib(df, pib_path=None, imacec_col='IMACEC_12sector', emp_col='Count_Workers', min_periods=6):
    # localizar archivo PIB por defecto en bases/IMACEC
    if pib_path is None:
        pib_path = os.path.join(bases_dir, 'IMACEC', 'IMACEC_PIB_base2018.csv')
    if not os.path.exists(pib_path):
        print(f'No encontré el archivo PIB en {pib_path}; salto cálculo de correlaciones vs PIB')
        return

    pib_df = pd.read_csv(pib_path)
    # detectar columna de valor (cualquiera que no sea la fecha)
    date_col = None
    for c in pib_df.columns:
        if c.lower() in ('date', 'fecha'):
            date_col = c
            break
    if date_col is None:
        # intentar asumir primera columna es fecha
        date_col = pib_df.columns[0]
    pib_df[date_col] = pd.to_datetime(pib_df[date_col])
    value_cols = [c for c in pib_df.columns if c != date_col]
    if not value_cols:
        print('No encontré columna de valores en el CSV PIB; abortando.')
        return
    pib_val_col = value_cols[0]
    pib_df = pib_df[[date_col, pib_val_col]].rename(columns={date_col: 'date', pib_val_col: 'PIB'})

    # Preparar resultados
    imacec_results = []
    emp_results = []

    for sector, data in df.groupby('MIP_sector_name'):
        sub = data.sort_values('Wage_Date')[[ 'Wage_Date', imacec_col, emp_col ]].copy()
        merged = sub.merge(pib_df, left_on='Wage_Date', right_on='date', how='left')
        # IMACEC vs PIB
        im_sub = merged[[imacec_col, 'PIB']].dropna()
        if len(im_sub) < min_periods:
            im_corr = float('nan')
        else:
            im_corr = im_sub[imacec_col].corr(im_sub['PIB'])
        imacec_results.append({'MIP_sector_name': sector, 'n_obs': len(im_sub), 'correlation_with_PIB': im_corr})

        # Empleo vs PIB
        emp_sub = merged[[emp_col, 'PIB']].dropna()
        if len(emp_sub) < min_periods:
            emp_corr = float('nan')
        else:
            emp_corr = emp_sub[emp_col].corr(emp_sub['PIB'])
        emp_results.append({'MIP_sector_name': sector, 'n_obs': len(emp_sub), 'correlation_with_PIB': emp_corr})

        # Wages vs PIB (nominal)
        if 'Avg_Taxable_Income' in merged.columns:
            wage_sub = merged[['Avg_Taxable_Income', 'PIB']].dropna()
            if len(wage_sub) < min_periods:
                wage_corr = float('nan')
            else:
                wage_corr = wage_sub['Avg_Taxable_Income'].corr(wage_sub['PIB'])
            imacec_results.append({'MIP_sector_name': sector, 'n_obs_wage': len(wage_sub), 'wage_corr_with_PIB': wage_corr})
        else:
            imacec_results.append({'MIP_sector_name': sector, 'n_obs_wage': 0, 'wage_corr_with_PIB': float('nan')})

        # Real wages vs PIB
        if 'Real_Avg_Wage' in merged.columns:
            real_wage_sub = merged[['Real_Avg_Wage', 'PIB']].dropna()
            if len(real_wage_sub) < min_periods:
                real_wage_corr = float('nan')
            else:
                real_wage_corr = real_wage_sub['Real_Avg_Wage'].corr(real_wage_sub['PIB'])
            emp_results.append({'MIP_sector_name': sector, 'n_obs_real_wage': len(real_wage_sub), 'real_wage_corr_with_PIB': real_wage_corr})
        else:
            emp_results.append({'MIP_sector_name': sector, 'n_obs_real_wage': 0, 'real_wage_corr_with_PIB': float('nan')})

    im_df = pd.DataFrame(imacec_results).sort_values('correlation_with_PIB', ascending=False)
    emp_df = pd.DataFrame(emp_results).sort_values('correlation_with_PIB', ascending=False)

    # Build wage vs PIB tables from the temp lists we appended
    wage_df = pd.DataFrame([{k:v for k,v in r.items() if k in ('MIP_sector_name','wage_corr_with_PIB','n_obs_wage')} for r in imacec_results]).sort_values('wage_corr_with_PIB', ascending=False)
    real_wage_df = pd.DataFrame([{k:v for k,v in r.items() if k in ('MIP_sector_name','real_wage_corr_with_PIB','n_obs_real_wage')} for r in emp_results]).sort_values('real_wage_corr_with_PIB', ascending=False)

    out_im_csv = os.path.join(tables_dir, 'imacec_vs_pib_correlation_by_sector.csv')
    out_emp_csv = os.path.join(tables_dir, 'employment_vs_pib_correlation_by_sector.csv')
    im_df.to_csv(out_im_csv, index=False)
    emp_df.to_csv(out_emp_csv, index=False)
    print(f'Saved IMACEC vs PIB correlations CSV: {out_im_csv}')
    print(f'Saved Employment vs PIB correlations CSV: {out_emp_csv}')
    # Exportar .tex coloreado para ambas tablas
    out_im_tex = os.path.join(tables_dir, 'imacec_vs_pib_correlation_by_sector.tex')
    out_emp_tex = os.path.join(tables_dir, 'employment_vs_pib_correlation_by_sector.tex')
    save_df_as_colored_tex(im_df, 'correlation_with_PIB', out_im_tex, caption='Correlación IMACEC sectorial vs PIB')
    save_df_as_colored_tex(emp_df, 'correlation_with_PIB', out_emp_tex, caption='Correlación empleo sectorial vs PIB')
    # Export wage vs PIB tables
    out_wage_csv = os.path.join(tables_dir, 'wage_vs_pib_correlation_by_sector.csv')
    out_wage_tex = os.path.join(tables_dir, 'wage_vs_pib_correlation_by_sector.tex')
    wage_df.to_csv(out_wage_csv, index=False)
    save_df_as_colored_tex(wage_df.rename(columns={'wage_corr_with_PIB':'correlation','n_obs_wage':'n_obs'}), 'correlation', out_wage_tex, caption='Correlación salario nominal vs PIB por sector')

    out_real_wage_csv = os.path.join(tables_dir, 'real_wage_vs_pib_correlation_by_sector.csv')
    out_real_wage_tex = os.path.join(tables_dir, 'real_wage_vs_pib_correlation_by_sector.tex')
    real_wage_df.to_csv(out_real_wage_csv, index=False)
    save_df_as_colored_tex(real_wage_df.rename(columns={'real_wage_corr_with_PIB':'correlation','n_obs_real_wage':'n_obs'}), 'correlation', out_real_wage_tex, caption='Correlación salario real vs PIB por sector')
    print(f'Saved wage–PIB tables: {out_wage_csv}, {out_real_wage_csv} and TEX.')


compute_correlations_vs_pib(df)


# Combinar y exportar tablas de volatilidad sectorial
def export_combined_volatility(vol_dir=None):
    if vol_dir is None:
        vol_dir = os.path.join(base, '7_volatilidades')
    files = {
        'vol_emp': os.path.join(vol_dir, 'vol_emp_sector_sa.csv'),
        'vol_nom': os.path.join(vol_dir, 'vol_nominal_wage_sector_sa.csv'),
        'vol_real': os.path.join(vol_dir, 'vol_real_wage_sector_sa.csv'),
    }
    parts = {}
    for key, path in files.items():
        if not os.path.exists(path):
            print(f'Volatility file not found: {path} (skipping)')
            continue
        dfv = pd.read_csv(path)
        # detect key column
        if 'MIP_sector_id' in dfv.columns:
            kcol = 'MIP_sector_id'
        elif 'MIP_sector_name' in dfv.columns:
            kcol = 'MIP_sector_name'
        else:
            # try first column as key
            kcol = dfv.columns[0]
        val_cols = [c for c in dfv.columns if c != kcol]
        if not val_cols:
            print(f'No value column found in {path}; skipping')
            continue
        val_col = val_cols[0]
        df_sub = dfv[[kcol, val_col]].copy()
        df_sub = df_sub.rename(columns={val_col: key, kcol: kcol})
        parts[key] = df_sub

    if not parts:
        print('No volatility parts found; nothing to export')
        return

    # choose merge key (prefer MIP_sector_id)
    merge_key = 'MIP_sector_id' if any('MIP_sector_id' in dfc.columns for dfc in parts.values()) else 'MIP_sector_name'
    merged = None
    for k, dfp in parts.items():
        # ensure merge_key exists in this part (try to rename if it's different)
        if merge_key not in dfp.columns:
            # try to find a name column and rename it
            possible = [c for c in dfp.columns if c.lower().startswith('mip')]
            if possible:
                dfp = dfp.rename(columns={possible[0]: merge_key})
            else:
                # fallback: use first column as key
                first = dfp.columns[0]
                dfp = dfp.rename(columns={first: merge_key})
        if merged is None:
            merged = dfp
        else:
            merged = merged.merge(dfp, on=merge_key, how='outer')

    # rename to final desired column names
    rename_map = {
        'vol_emp': 'vol_emp_sector_sa',
        'vol_nom': 'vol_nominal_wage_sector_sa',
        'vol_real': 'vol_real_wage_sector_sa'
    }
    merged = merged.rename(columns=rename_map)

    # Save CSV to output tables
    out_csv = os.path.join(tables_dir, 'combined_volatility_by_sector.csv')
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    merged.to_csv(out_csv, index=False)
    print('Saved combined volatility CSV:')
    print(' -', out_csv)

    # Export LaTeX colored table (color positive green, negative red)
    out_tex = os.path.join(tables_dir, 'combined_volatility_by_sector.tex')
    # try to pick a sensible numeric column to color (first numeric volatility)
    num_cols = [c for c in merged.columns if c not in (merge_key,)]
    color_col = num_cols[0] if num_cols else None
    # For volatility table do not apply green/red coloring
    save_df_as_tex(merged, out_tex, caption='Volatilidades sectoriales (SA)')


export_combined_volatility()


def create_volatility_master_table(vol_dir=None, out_dir=None):
    """Create a master table with all volatilities and export as CSV and LaTeX.

    Columns: Volatility, Sector, Abbrev, Value
    """
    from subroutines.transitions_prep import get_state_abbrev_map

    if vol_dir is None:
        vol_dir = os.path.join(base, '7_volatilidades')
    if out_dir is None:
        out_dir = tables_dir
    os.makedirs(out_dir, exist_ok=True)

    rows = []

    # Helper to read a sector volatility CSV and append rows
    def _append_sector_vol(path, volatility_name, key_names=None):
        if not os.path.exists(path):
            print(f'Sector volatility file not found: {path} (skipping)')
            return
        dfv = pd.read_csv(path)
        # detect sector column
        if 'MIP_sector_name' in dfv.columns:
            sector_col = 'MIP_sector_name'
        elif 'MIP_sector_id' in dfv.columns:
            sector_col = 'MIP_sector_id'
        else:
            sector_col = dfv.columns[0]
        # detect value column (first numeric or non-key)
        val_cols = [c for c in dfv.columns if c != sector_col]
        if not val_cols:
            return
        val_col = val_cols[0]
        for _, r in dfv.iterrows():
            sector = r.get(sector_col)
            value = r.get(val_col)
            rows.append({'Volatility': volatility_name, 'Sector': sector, 'Value': value})

    # Sector-level volatilities (already computed)
    _append_sector_vol(os.path.join(vol_dir, 'vol_emp_sector_sa.csv'), 'Volatilidad Empleo (SA)')
    _append_sector_vol(os.path.join(vol_dir, 'vol_nominal_wage_sector_sa.csv'), 'Volatilidad Salario Nominal (SA)')
    _append_sector_vol(os.path.join(vol_dir, 'vol_real_wage_sector_sa.csv'), 'Volatilidad Salario Real (SA)')

    # Aggregated totals (single-value CSVs)
    def _append_total_vol(path, volatility_name):
        if not os.path.exists(path):
            print(f'Total volatility file not found: {path} (skipping)')
            return
        dft = pd.read_csv(path)
        # pick first numeric column
        numcols = dft.select_dtypes(include=[np.number]).columns.tolist()
        if numcols:
            val = dft[numcols[0]].iloc[0]
        else:
            # fallback: first non-index column
            cols = [c for c in dft.columns]
            val = dft[cols[0]].iloc[0]
        rows.append({'Volatility': volatility_name, 'Sector': 'Agregada', 'Value': val})

    _append_total_vol(os.path.join(vol_dir, 'vol_emp_total_sa.csv'), 'Volatilidad Empleo (SA)')
    _append_total_vol(os.path.join(vol_dir, 'vol_nominal_wage_total_sa.csv'), 'Volatilidad Salario Nominal (SA)')
    _append_total_vol(os.path.join(vol_dir, 'vol_real_wage_total_sa.csv'), 'Volatilidad Salario Real (SA)')

    # Growth volatilities: prefer precomputed volatility CSVs (saved by 1_Statistics)
    _append_sector_vol(os.path.join(vol_dir, 'vol_emp_growth_sector_sa.csv'), 'Volatilidad Crecimiento Empleo (SA)')
    _append_total_vol(os.path.join(vol_dir, 'vol_emp_growth_total_sa.csv'), 'Volatilidad Crecimiento Empleo (SA)')

    _append_sector_vol(os.path.join(vol_dir, 'vol_nominal_wage_growth_sector_sa.csv'), 'Volatilidad Crecimiento Salario Nominal (SA)')
    _append_total_vol(os.path.join(vol_dir, 'vol_nominal_wage_growth_total_sa.csv'), 'Volatilidad Crecimiento Salario Nominal (SA)')

    _append_sector_vol(os.path.join(vol_dir, 'vol_real_wage_growth_sector_sa.csv'), 'Volatilidad Crecimiento Salario Real (SA)')
    _append_total_vol(os.path.join(vol_dir, 'vol_real_wage_growth_total_sa.csv'), 'Volatilidad Crecimiento Salario Real (SA)')

    # Build DataFrame
    if not rows:
        print('No volatility rows collected; aborting master table creation')
        return
    master = pd.DataFrame(rows)

    # Normalize sector column to strings and map abbreviations when possible
    m = get_state_abbrev_map(model='13')
    # If sectors are ids, map using known MIP mapping from transitions_prep.prepare
    mip_map = {
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
    def _to_name(x):
        if pd.isna(x):
            return x
        try:
            if isinstance(x, (int, float)) and not isinstance(x, bool) and not pd.isna(x):
                xi = int(x)
                return mip_map.get(xi, str(x))
        except Exception:
            pass
        return str(x)

    master['Sector'] = master['Sector'].apply(_to_name)
    master['Abbrev'] = master['Sector'].map(lambda s: m.get(s, ''))
    master['Value'] = master['Value'].astype(float)
    master = master[['Volatility', 'Sector', 'Abbrev', 'Value']].sort_values(['Volatility', 'Sector'])

    out_csv = os.path.join(out_dir, 'volatility_master_table.csv')
    out_tex = os.path.join(out_dir, 'volatility_master_table.tex')
    master.to_csv(out_csv, index=False)
    # Export plain LaTeX table
    save_df_as_tex(master, out_tex, caption='Tabla maestra de volatilidades (SA)')
    print(f'Saved master volatility table: {out_csv} and {out_tex}')


create_volatility_master_table()
