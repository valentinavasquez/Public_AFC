import os

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
MPLCONFIGDIR = os.path.join(PROJECT_ROOT, "tmp", "matplotlib")
os.makedirs(MPLCONFIGDIR, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", MPLCONFIGDIR)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import colors

from pyspark.sql import functions as F

from .transition_outputs import (
    DEFAULT_OUTPUT_BASE,
    compute_transition_counts,
    compute_transition_probabilities,
    compute_wide_counts,
    compute_wide_probabilities,
    get_transitions,
    output_path,
)
from .transitions_prep import (
    abbreviate_transition_df,
    prepare,
    reorder_transition_csv,
    write_single_csv,
)


def output_dir_for_moves(state_count, by_period=False, output_base=None):
    output_base = output_base or DEFAULT_OUTPUT_BASE
    suffix = f"transitions_moves_{int(state_count)}state"
    if by_period:
        suffix = f"{suffix}_by_period"

    out_dir = os.path.join(output_base, suffix)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def write_move_csv(df, out_dir, name, state_count, by_period=False):
    csv_path = output_path(out_dir, name, state_count, "csv")
    write_single_csv(df, csv_path)
    reorder_transition_csv(csv_path, model=str(state_count), by_period=by_period)
    return csv_path


def compute_moves(transitions):
    return transitions.filter(F.col("State_From") != F.col("State_To"))


def export_moves_heatmap(out_dir, state_count):
    csv_path = output_path(out_dir, "probabilities_wide", state_count, "csv")
    pdf_path = output_path(out_dir, "probabilities_wide_heatmap", state_count, "pdf")

    df = pd.read_csv(csv_path)
    if "State_From" in df.columns:
        mat = df.set_index("State_From")
    else:
        mat = df.set_index(df.columns[0])

    mat = mat.apply(pd.to_numeric, errors="coerce").fillna(0)

    n_rows = mat.shape[0]
    n_cols = mat.shape[1]
    cell_size = 0.8 if n_rows >= 24 else 0.5
    figsize_x = max(12, n_cols * cell_size)
    figsize_y = max(10, n_rows * cell_size)

    fig, ax = plt.subplots(figsize=(figsize_x, figsize_y))

    vmin = 0
    vmax = float(mat.values.max()) if mat.values.size else 1.0
    norm = colors.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.get_cmap("viridis")

    im = ax.imshow(
        mat.values,
        aspect="auto",
        cmap=cmap,
        norm=norm,
    )

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label(
        "Transition probability (given move)",
        fontsize=11,
        fontweight="bold",
    )

    ax.set_xticks(range(len(mat.columns)))
    ax.set_xticklabels(
        mat.columns,
        rotation=90,
        fontsize=8,
        fontweight="bold",
    )

    ax.set_yticks(range(len(mat.index)))
    ax.set_yticklabels(
        mat.index,
        fontsize=8,
        fontweight="bold",
    )

    if n_rows <= 14:
        number_size = 7
    elif n_rows <= 24:
        number_size = 6
    else:
        number_size = 5

    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            val = mat.iat[i, j]
            r, g, b, _ = cmap(norm(val))
            luminance = 0.299 * r + 0.587 * g + 0.114 * b
            text_color = "white" if luminance < 0.5 else "black"

            ax.text(
                j,
                i,
                f"{val:.3f}",
                ha="center",
                va="center",
                fontsize=number_size,
                fontweight="bold",
                color=text_color,
            )

    ax.set_xlabel("")
    ax.set_ylabel("")

    fig.tight_layout()
    fig.savefig(pdf_path, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    return pdf_path


def export_moves_counts_latex(out_dir, state_count):
    counts_path = output_path(out_dir, "counts_wide", state_count, "csv")
    df = pd.read_csv(counts_path)

    try:
        df = abbreviate_transition_df(df, model=str(state_count), by_period=False)
    except Exception as exc:
        print(f"Could not abbreviate transition table: {exc}")

    tex = df.to_latex(index=False, na_rep="0", longtable=True)
    tex_path = output_path(out_dir, "counts_wide", state_count, "tex")
    with open(tex_path, "w", encoding="utf-8") as f:
        f.write(tex)
    return tex_path


def export_destination_summary(out_dir, state_count):
    counts_path = output_path(out_dir, "counts_wide", state_count, "csv")
    df_raw = pd.read_csv(counts_path)
    from_col = "State_From" if "State_From" in df_raw.columns else df_raw.columns[0]
    dest_cols = [c for c in df_raw.columns if c != from_col]
    df_num = df_raw[dest_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    totals = df_num.sum(axis=1)
    columns = [
        "Sector From",
        "Sector To",
        "Cantidad Trabajadores",
        "Total trabajadores que se movieron",
    ]
    records = []

    for i, row in df_raw.iterrows():
        sector_from = row[from_col]
        total_from = int(totals.iloc[i])
        for col in dest_cols:
            raw_val = pd.to_numeric(row[col], errors="coerce")
            val = 0 if pd.isna(raw_val) else int(raw_val)
            if val > 0:
                records.append({
                    "Sector From": sector_from,
                    "Sector To": col,
                    "Cantidad Trabajadores": val,
                    "Total trabajadores que se movieron": total_from,
                })

    dest_table = pd.DataFrame(records, columns=columns)
    if not dest_table.empty:
        dest_table = dest_table.sort_values(
            ["Sector From", "Cantidad Trabajadores"],
            ascending=[True, False],
        ).reset_index(drop=True)

    csv_path = output_path(out_dir, "destination_summary", state_count, "csv")
    tex_path = output_path(out_dir, "destination_summary", state_count, "tex")
    dest_table.to_csv(csv_path, index=False)

    tex = dest_table.to_latex(index=False, na_rep="0", longtable=True)
    with open(tex_path, "w", encoding="utf-8") as f:
        f.write(tex)

    return csv_path, tex_path


def run_transition_moves_outputs(
    state_count,
    by_period=False,
    output_base=None,
    ctx=None,
):
    state_count = int(state_count)
    ctx = ctx or prepare()
    transitions = get_transitions(ctx, state_count)
    moves = compute_moves(transitions)
    out_dir = output_dir_for_moves(
        state_count,
        by_period=by_period,
        output_base=output_base,
    )

    transition_counts = compute_transition_counts(moves, by_period=by_period)
    transition_counts_wide = compute_wide_counts(transition_counts, by_period=by_period)
    transition_probs = compute_transition_probabilities(transition_counts, by_period=by_period)
    transition_probabilities_wide = compute_wide_probabilities(
        transition_probs,
        by_period=by_period,
    )

    if by_period:
        prob_name = "probabilities_by_period_wide"
        count_name = "counts_by_period_wide"
    else:
        prob_name = "probabilities_wide"
        count_name = "counts_wide"

    prob_path = write_move_csv(
        transition_probabilities_wide,
        out_dir,
        prob_name,
        state_count,
        by_period=by_period,
    )
    count_path = write_move_csv(
        transition_counts_wide,
        out_dir,
        count_name,
        state_count,
        by_period=by_period,
    )

    outputs = {
        "output_dir": out_dir,
        "probabilities": prob_path,
        "counts": count_path,
    }

    if not by_period:
        outputs["heatmap"] = export_moves_heatmap(out_dir, state_count)
        outputs["counts_tex"] = export_moves_counts_latex(out_dir, state_count)
        destination_csv, destination_tex = export_destination_summary(out_dir, state_count)
        outputs["destination_summary_csv"] = destination_csv
        outputs["destination_summary_tex"] = destination_tex

    print(f"Saved {state_count}-state move outputs in: {out_dir}")
    return outputs


def run_transition_moves_family(
    states=(13, 14, 24, 25),
    include_aggregate=True,
    include_by_period=True,
    output_base=None,
):
    ctx = prepare()
    outputs = {}

    for state_count in states:
        outputs[state_count] = {}
        if include_aggregate:
            outputs[state_count]["aggregate"] = run_transition_moves_outputs(
                state_count,
                by_period=False,
                output_base=output_base,
                ctx=ctx,
            )
        if include_by_period:
            outputs[state_count]["by_period"] = run_transition_moves_outputs(
                state_count,
                by_period=True,
                output_base=output_base,
                ctx=ctx,
            )

    return outputs
