import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
MPLCONFIGDIR = os.path.join(PROJECT_ROOT, "tmp", "matplotlib")
os.makedirs(MPLCONFIGDIR, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", MPLCONFIGDIR)

import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import colors

from pyspark.sql import Window
from pyspark.sql import functions as F

from .transitions_prep import (
    abbreviate_transition_df,
    prepare,
    reorder_transition_csv,
    write_single_csv,
)


DEFAULT_OUTPUT_BASE = os.path.join(PROJECT_ROOT, "output", "Industrial_Sectors")

TRANSITION_KEYS = {
    13: "transitions_13",
    14: "transitions_14",
    24: "transitions_24",
    25: "transitions_25",
}


def output_dir_for_state(state_count, output_base=None):
    output_base = output_base or DEFAULT_OUTPUT_BASE
    out_dir = os.path.join(output_base, f"transitions_{state_count}state")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def state_suffix(state_count):
    return f"{state_count}state"


def output_path(out_dir, name, state_count, extension):
    return os.path.join(out_dir, f"{name}_{state_suffix(state_count)}.{extension}")


def get_transitions(ctx, state_count):
    try:
        return ctx[TRANSITION_KEYS[int(state_count)]]
    except KeyError as exc:
        valid = ", ".join(str(k) for k in sorted(TRANSITION_KEYS))
        raise ValueError(f"state_count must be one of: {valid}") from exc


def compute_transition_counts(transitions, by_period=False):
    group_cols = ["State_From", "State_To"]
    if by_period:
        group_cols = ["Wage_Date"] + group_cols

    return transitions.groupBy(*group_cols).agg(F.count("*").alias("count"))


def compute_wide_counts(transition_counts, by_period=False):
    group_cols = ["State_From"]
    order_cols = ["State_From"]
    if by_period:
        group_cols = ["Wage_Date"] + group_cols
        order_cols = ["Wage_Date"] + order_cols

    return (
        transition_counts
        .groupBy(*group_cols)
        .pivot("State_To")
        .agg(F.first("count"))
        .fillna(0)
        .orderBy(*order_cols)
    )


def compute_transition_probabilities(transition_counts, by_period=False):
    partition_cols = ["State_From"]
    order_cols = ["State_From", "State_To"]
    if by_period:
        partition_cols = ["Wage_Date"] + partition_cols
        order_cols = ["Wage_Date"] + order_cols

    w_from = Window.partitionBy(*partition_cols)

    return (
        transition_counts
        .withColumn("total_from", F.sum("count").over(w_from))
        .withColumn("probability", F.round(F.col("count") / F.col("total_from"), 6))
        .orderBy(*order_cols)
    )


def compute_wide_probabilities(transition_probs, by_period=False):
    select_cols = ["State_From", "State_To", "probability"]
    group_cols = ["State_From"]
    order_cols = ["State_From"]
    if by_period:
        select_cols = ["Wage_Date"] + select_cols
        group_cols = ["Wage_Date"] + group_cols
        order_cols = ["Wage_Date"] + order_cols

    return (
        transition_probs
        .select(*select_cols)
        .groupBy(*group_cols)
        .pivot("State_To")
        .agg(F.first("probability"))
        .fillna(0)
        .orderBy(*order_cols)
    )


def write_transition_csv(df, out_dir, name, state_count, by_period=False):
    csv_path = output_path(out_dir, name, state_count, "csv")
    write_single_csv(df, csv_path)
    reorder_transition_csv(
        csv_path,
        model=str(state_count),
        by_period=by_period,
    )
    return csv_path


def export_absorption_ranking(transition_counts, out_dir, state_count, topn=5):
    w_rank = Window.partitionBy("State_From").orderBy(F.col("count").desc())
    w_from = Window.partitionBy("State_From")

    rank_df = (
        transition_counts
        .withColumn("total_from", F.sum("count").over(w_from))
        .withColumn("Share", F.col("count") / F.col("total_from"))
        .withColumn("Rank", F.row_number().over(w_rank))
        .orderBy("State_From", "Rank")
    )

    ranking_path = write_transition_csv(
        rank_df.select("State_From", "State_To", "count", "Share", "Rank"),
        out_dir,
        "absorption_ranking_long",
        state_count,
        by_period=False,
    )

    longp = pd.read_csv(ranking_path)
    wide_rows = []

    for state_from, group in longp.groupby("State_From"):
        top = group.nsmallest(topn, "Rank")
        row = {"State_From": state_from}

        for i, (_, r) in enumerate(top.iterrows(), start=1):
            row[f"Top{i}_To"] = r["State_To"]
            row[f"Top{i}_Count"] = int(r["count"])
            row[f"Top{i}_Share"] = float(r["Share"])

        wide_rows.append(row)

    wide_path = output_path(
        out_dir,
        f"absorption_ranking_wide_top{topn}",
        state_count,
        "csv",
    )
    pd.DataFrame(wide_rows).to_csv(wide_path, index=False)


def export_total_incoming(out_dir, state_count):
    counts_path = output_path(out_dir, "counts_wide", state_count, "csv")
    counts_df = pd.read_csv(counts_path)

    state_col = "State_From" if "State_From" in counts_df.columns else counts_df.columns[0]
    destination_cols = [c for c in counts_df.columns if c != state_col]

    incoming = (
        counts_df[destination_cols]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .sum(axis=0)
        .sort_values(ascending=False)
        .reset_index()
    )

    incoming.columns = ["State_To", "Total_Incoming"]
    total = incoming["Total_Incoming"].sum()
    incoming["Share"] = incoming["Total_Incoming"] / total if total else 0

    incoming_path = output_path(out_dir, "absorption_total_incoming", state_count, "csv")
    incoming.to_csv(incoming_path, index=False)


def export_heatmap(out_dir, state_count):
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
        "Transition probability",
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


def export_counts_latex(out_dir, state_count):
    counts_path = output_path(out_dir, "counts_wide", state_count, "csv")
    df = pd.read_csv(counts_path)
    df = abbreviate_transition_df(df, model=str(state_count), by_period=False)

    tex = df.to_latex(
        index=False,
        na_rep="0",
        longtable=True,
        escape=True,
    )

    tex_path = output_path(out_dir, "counts_wide", state_count, "tex")
    with open(tex_path, "w", encoding="utf-8") as f:
        f.write(tex)

    return tex_path


def run_transition_outputs(
    state_count,
    by_period=False,
    output_base=None,
    ctx=None,
    topn=5,
):
    state_count = int(state_count)
    ctx = ctx or prepare()
    transitions = get_transitions(ctx, state_count)
    out_dir = output_dir_for_state(state_count, output_base=output_base)

    transition_counts = compute_transition_counts(transitions, by_period=by_period)
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

    prob_path = write_transition_csv(
        transition_probabilities_wide,
        out_dir,
        prob_name,
        state_count,
        by_period=by_period,
    )
    count_path = write_transition_csv(
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
        export_absorption_ranking(transition_counts, out_dir, state_count, topn=topn)
        export_total_incoming(out_dir, state_count)
        outputs["heatmap"] = export_heatmap(out_dir, state_count)
        outputs["counts_tex"] = export_counts_latex(out_dir, state_count)

    print(f"Saved {state_count}-state outputs in: {out_dir}")
    return outputs


def run_transition_family(
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
            outputs[state_count]["aggregate"] = run_transition_outputs(
                state_count,
                by_period=False,
                output_base=output_base,
                ctx=ctx,
            )
        if include_by_period:
            outputs[state_count]["by_period"] = run_transition_outputs(
                state_count,
                by_period=True,
                output_base=output_base,
                ctx=ctx,
            )

    return outputs
