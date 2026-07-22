#!/usr/bin/env python3
"""
Publication figures for the KSPFail paper. Every value below is hard-coded from a
committed source (cited inline) so the figures are reproducible from the repo alone.
Run: .venv/bin/python results/figures/make_figures.py

Figures produced (results/figures/):
  fig2_architecture.png          - system-under-test topology + fault-injection points
  fig3_executor_oom_ramp.png     - memory ramp vs 900MB threshold vs OOM limit
  fig4_baseline_leadtime.png     - static-threshold lead time per class (only exec_oom warns)
  fig5_executor_oom_n8_n15.png   - N=8 vs N=15 null result (model never beats trivial baseline)

(Figure 1 is the existing SHAP summary, results/ml-first-pass/disk_pressure_shap_summary.png.)
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

OUT = os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({
    "font.size": 11, "axes.titlesize": 12, "axes.labelsize": 11,
    "axes.spines.top": False, "axes.spines.right": False,
    "figure.dpi": 120, "savefig.dpi": 300, "savefig.bbox": "tight",
})
C = {"blue": "#2c6fbb", "red": "#c0392b", "green": "#2e7d32",
     "grey": "#6b6b6b", "orange": "#e08e0b", "light": "#d9e4f0"}


def save(fig, name):
    for ext in ("png", "pdf"):
        fig.savefig(os.path.join(OUT, f"{name}.{ext}"))
    plt.close(fig)
    print("wrote", name)


# ---------------------------------------------------------------- Fig 2: architecture
def fig_architecture():
    # Topology from infra/: Kind single node; Strimzi Kafka 4.3.0 (1 broker, JMX);
    # Spark 4.1.2 Structured Streaming (driver+executors); Prometheus scrapes
    # JMX / cAdvisor / node-exporter; producer load-gen. Fault targets from fault_injection/*.py.
    fig, ax = plt.subplots(figsize=(9.2, 5.4))
    ax.set_xlim(0, 100); ax.set_ylim(0, 100); ax.axis("off")

    def box(x, y, w, h, text, fc, ec=C["grey"], fs=10, bold=False):
        p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.6,rounding_size=2",
                           fc=fc, ec=ec, lw=1.4)
        ax.add_patch(p)
        ax.text(x + w / 2, y + h / 2, text, ha="center", va="center",
                fontsize=fs, fontweight="bold" if bold else "normal", zorder=5)

    def arrow(x1, y1, x2, y2, color=C["grey"], style="-|>", ls="-", lw=1.5):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle=style,
                     mutation_scale=14, color=color, lw=lw, linestyle=ls, zorder=1))

    # Kind cluster boundary
    ax.add_patch(FancyBboxPatch((2, 30), 96, 62, boxstyle="round,pad=0.6,rounding_size=2",
                 fc="none", ec=C["blue"], lw=1.6, linestyle=(0, (6, 4))))
    ax.text(4, 88, "Kind single-node Kubernetes cluster", fontsize=10,
            color=C["blue"], fontweight="bold", ha="left")

    # data plane
    box(6, 60, 16, 12, "Producer\nload-gen", C["light"])
    box(30, 60, 20, 12, "Kafka broker\n(Strimzi 4.3.0, ×1)\n+ JMX exporter", C["light"], bold=True)
    box(58, 60, 22, 12, "Spark 4.1.2\nStructured Streaming\n(driver + executors)", C["light"], bold=True)
    arrow(22, 66, 30, 66); arrow(50, 66, 58, 66)

    # monitoring plane
    box(30, 35, 50, 12, "Prometheus\n(scrapes JMX · cAdvisor · node-exporter · 60 s interval)", "#eef3d9", bold=True)
    for x in (40, 55, 69):
        arrow(x, 60, x, 47, color=C["green"], ls=(0, (2, 2)), lw=1.2)

    # fault-injection plane
    box(6, 6, 26, 14, "Fault injection\n(fault_injection/*.py)\nreal, physical faults", "#f6ded9",
        ec=C["red"], bold=True)
    faults = [
        (40, "broker_kill", 40, 60),
        (55, "executor_oom", 66, 60),
        (69, "disk_pressure\n(node /var)", 69, 46),
        (30, "network_degradation", 32, 60),
    ]
    ax.text(50, 22, "targets:", fontsize=9, color=C["red"], ha="center")
    labels = ["broker_kill → Kafka", "executor_oom → Spark exec",
              "disk_pressure → node fs", "network_degradation → broker net",
              "backpressure_cascade → Spark streaming (excluded: no independent metric)"]
    for i, t in enumerate(labels):
        ax.text(34, 18 - i * 3.0, "• " + t, fontsize=8.2, color=C["red"], ha="left", va="center")
    arrow(19, 20, 40, 60, color=C["red"], ls=(0, (3, 3)), lw=1.0)
    arrow(19, 20, 66, 60, color=C["red"], ls=(0, (3, 3)), lw=1.0)

    # No in-image title — caption is supplied by the LaTeX \caption{} in the paper.
    save(fig, "fig2_architecture")


# ---------------------------------------------------------------- Fig 3: exec_oom ramp
def fig_executor_oom_ramp():
    # Source: results/baseline-threshold-evidence/prometheus_derived_figures.json
    #   ramptest6_full_ramp_sequence_mb = [395.7, 642.0, 822.5, 1074.4]  (step=15s)
    #   threshold 900 MB; cgroup limit 1207959552 B = 1152 MiB ≈ 1208 MB
    ramp = [395.7, 642.0, 822.5, 1074.4]
    t = [0, 15, 30, 45]  # successive samples, 15 s apart (real step)
    thr, limit = 900, 1207959552 / 1e6

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(t, ramp, "-o", color=C["blue"], lw=2, ms=8, label="executor working-set memory (ramptest6)")
    ax.axhline(thr, color=C["orange"], ls="--", lw=1.6)
    ax.axhline(limit, color=C["red"], ls="--", lw=1.6)
    ax.text(0.3, thr + 12, "static threshold = 900 MB (≈75% of limit)", color=C["orange"], fontsize=9)
    ax.text(0.3, limit + 12, "cgroup limit ≈ 1208 MB → OOM kill", color=C["red"], fontsize=9)
    for xi, yi in zip(t, ramp):
        ax.annotate(f"{yi:.0f}", (xi, yi), textcoords="offset points", xytext=(6, -12), fontsize=9)
    # threshold crossing occurs between sample 3 (822) and 4 (1074)
    ax.axvspan(30, 45, color=C["orange"], alpha=0.08)
    ax.set_xlabel("Successive Prometheus samples before OOM kill (15 s apart)")
    ax.set_ylabel("Memory (MB)")
    ax.set_ylim(300, 1320)
    ax.set_xticks(t)
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    # No in-image title — caption is supplied by the LaTeX \caption{} in the paper.
    save(fig, "fig3_executor_oom_ramp")


# ---------------------------------------------------------------- Fig 4: baseline lead time
def fig_baseline_leadtime():
    # Source: docs/baseline_thresholds.md §2 (exec_oom per-rep) + "Full Evaluation Results"
    # exec_oom crossing->OOM leads (ramptest3..10); disk/network severity leads; broker N/A.
    data = {
        "broker_kill": None,                         # N/A: 12.5% recall, crossing == outage
        "disk_pressure": [0, 0, 0, 0, 0, 0, 0, 0],   # severity coincides w/ detection, 8/8
        "network_degradation": [0, 0, 0, 65],        # only 4/8 reach severity
        "executor_oom": [59, 65, 83, 48, 64, 65, 77, 58],  # 8 reps
    }
    order = ["broker_kill", "disk_pressure", "network_degradation", "executor_oom"]
    show_mean = {"disk_pressure", "executor_oom"}   # mean is meaningful only here
    fig, ax = plt.subplots(figsize=(7.8, 4.6))
    rng = np.random.default_rng(3)
    for i, k in enumerate(order):
        v = data[k]
        if v is None:                                # broker_kill: not a lead time
            ax.text(i, 36, "N/A", ha="center", va="center", fontsize=12,
                    color=C["grey"], style="italic", fontweight="bold")
            ax.text(i, 28, "recall 12.5%\ncrossing = outage", ha="center", va="center",
                    fontsize=8, color=C["grey"], style="italic")
            continue
        xs = i + rng.uniform(-0.12, 0.12, len(v))
        ax.scatter(xs, v, s=55, color=C["blue"], alpha=0.85, zorder=3, edgecolor="white")
        if k in show_mean:
            m = float(np.mean(v))
            ax.plot([i - 0.24, i + 0.24], [m, m], color=C["red"], lw=2.4, zorder=4)
            ax.text(i + 0.28, m, f"mean {m:.1f} s", color=C["red"], fontsize=9, va="center")
    # network_degradation: honest per-value breakdown, NOT a misleading 16 s mean of [0,0,0,65]
    ax.text(2, 74, "0 s ×3, 65 s ×1\n(only 4/8 reach severity)", ha="center",
            fontsize=8.5, color=C["grey"])
    ax.set_xticks(range(len(order)))
    ax.set_xticklabels([k.replace("_", "\n") for k in order])
    ax.set_ylabel("Static-threshold lead time (s)")
    ax.set_xlim(-0.95, 3.6)
    ax.set_ylim(-8, 92)
    ax.axhline(0, color=C["grey"], lw=0.8)
    # No in-image title — caption is supplied by the LaTeX \caption{} in the paper.
    ax.text(-0.9, -6.8, "backpressure_cascade excluded (no independently-scraped lag metric)",
            fontsize=8, color=C["grey"], ha="left")
    save(fig, "fig4_baseline_leadtime")


# ---------------------------------------------------------------- Fig 5: N8 vs N15 null
def fig_executor_oom_n8_n15():
    # Source: docs/paper_draft.md §3.4 + results/ml-first-pass/executor_oom_n15_evaluation.json
    #   N=8 (5-episode set): model F1 0.800, trivial baseline 0.909, p 0.48
    #   N=15 (11-episode set): model F1 0.909, trivial baseline 0.957, p 0.500
    groups = ["N = 8\n(5 episodes)", "N = 15\n(11 episodes)"]
    model = [0.800, 0.909]
    trivial = [0.909, 0.957]
    pvals = [0.48, 0.500]
    x = np.arange(len(groups)); w = 0.36
    fig, ax = plt.subplots(figsize=(6.8, 4.4))
    b1 = ax.bar(x - w / 2, model, w, label="RandomForest F1 (LOO-CV)", color=C["blue"])
    b2 = ax.bar(x + w / 2, trivial, w, label="trivial always-pre-failure baseline",
                color=C["grey"], alpha=0.65)
    for b in list(b1) + list(b2):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.012,
                f"{b.get_height():.3f}", ha="center", fontsize=9)
    for i, p in enumerate(pvals):
        ax.text(x[i], 1.03, f"permutation p = {p:.2f}", ha="center", fontsize=9, color=C["red"])
    ax.set_ylim(0, 1.14)
    ax.set_xticks(x); ax.set_xticklabels(groups)
    ax.set_ylabel("F1")
    ax.legend(loc="lower center", frameon=False, fontsize=9, bbox_to_anchor=(0.5, -0.32), ncol=1)
    # No in-image title — caption is supplied by the LaTeX \caption{} in the paper.
    save(fig, "fig5_executor_oom_n8_n15")


if __name__ == "__main__":
    fig_architecture()
    fig_executor_oom_ramp()
    fig_baseline_leadtime()
    fig_executor_oom_n8_n15()
    print("\nAll figures written to", OUT)
