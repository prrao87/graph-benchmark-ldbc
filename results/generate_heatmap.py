#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import re
from pathlib import Path

HEADER_RE = re.compile(r"Name \(time in (?P<unit>[^)]+)\)")
UNIT_TO_MS = {
    "s": 1000.0,
    "ms": 1.0,
    "us": 0.001,
    "µs": 0.001,
    "ns": 0.000001,
}


def normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_benchmark_file(path: Path) -> dict[str, float]:
    unit_scale: float | None = None
    means_ms: dict[str, float] = {}

    for line in path.read_text().splitlines():
        if unit_scale is None:
            match = HEADER_RE.search(line)
            if match:
                unit = match.group("unit").strip()
                unit_scale = UNIT_TO_MS.get(unit)
                if unit_scale is None:
                    raise ValueError(f"Unsupported time unit '{unit}' in {path.name}")
                continue

        if not line.startswith("test_"):
            continue

        if unit_scale is None:
            raise ValueError(f"Missing header with time unit in {path.name}")

        columns = re.split(r"\s{2,}", line.strip())
        if len(columns) < 4:
            continue

        name = columns[0]
        mean_token = columns[3].split()[0].replace(",", "")
        means_ms[name] = float(mean_token) * unit_scale

    if unit_scale is None:
        raise ValueError(f"Missing header with time unit in {path.name}")
    return means_ms


def sort_query_key(name: str) -> tuple[int, int | str]:
    match = re.search(r"query(\d+)", name)
    if match:
        return (0, int(match.group(1)))
    return (1, name)


def _find_result_files(results_dir: Path) -> list[Path]:
    skip = {
        "compare.py",
        "generate_dots.py",
        "generate_heatmap.py",
        "benchmark_plot.png",
        "benchmark_dots.png",
        "benchmark_heatmap.png",
    }
    files: list[Path] = []
    for path in sorted(results_dir.iterdir()):
        if not path.is_file():
            continue
        if path.name in skip:
            continue
        if path.suffix in {".py", ".png"}:
            continue
        files.append(path)
    return files


def _nice_log_ticks(min_ms: float, max_ms: float) -> list[float]:
    if not (min_ms > 0 and max_ms > 0):
        return []
    min_exp = math.floor(math.log10(min_ms))
    max_exp = math.ceil(math.log10(max_ms))
    ticks: list[float] = []
    for exp in range(min_exp, max_exp + 1):
        for mult in (1, 2, 5):
            val = mult * (10**exp)
            if min_ms <= val <= max_ms:
                ticks.append(val)
    if not ticks:
        ticks = [min_ms, max_ms]
    # Keep a small number of ticks.
    if len(ticks) > 8:
        step = max(1, len(ticks) // 8)
        ticks = ticks[::step]
    return sorted(set(ticks))


def plot_heatmap(
    systems: list[str],
    queries: list[str],
    values_by_query: list[list[float | None]],
    output_path: Path,
    *,
    annotate_extremes: bool,
) -> None:
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError as exc:
        raise SystemExit(
            "matplotlib (and numpy) are required for plotting. Install them and retry."
        ) from exc

    rows = len(queries)
    cols = len(systems)
    fig_h = max(7.0, 0.32 * rows + 2.0)
    fig_w = max(10.0, 0.75 * cols + 6.0)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    # Build matrix (ms), then transform to log10(ms) for the heatmap color.
    ms = np.full((rows, cols), np.nan, dtype=float)
    for r, row in enumerate(values_by_query):
        for c, value in enumerate(row):
            if value is None:
                continue
            ms[r, c] = float(value)

    with np.errstate(divide="ignore", invalid="ignore"):
        log_ms = np.log10(ms)

    masked = np.ma.masked_invalid(log_ms)

    finite_log = log_ms[np.isfinite(log_ms)]
    if finite_log.size:
        # Use percentiles so the map "lights up" primarily for slow queries.
        vmin = float(np.percentile(finite_log, 20))
        vmax = float(np.percentile(finite_log, 95))
        if not (vmin < vmax):
            vmin = float(np.nanmin(finite_log))
            vmax = float(np.nanmax(finite_log))
    else:
        vmin, vmax = 0.0, 1.0

    from matplotlib.colors import LinearSegmentedColormap

    # Transparent for low values, pastel red for high values.
    # Keep early stops fully transparent so "fast" cells fade away.
    cmap = LinearSegmentedColormap.from_list(
        "transparent_red",
        [
            (0.00, (1.0, 1.0, 1.0, 0.0)),
            (0.55, (1.0, 1.0, 1.0, 0.0)),
            (1.00, (0.96, 0.56, 0.56, 0.90)),
        ],
    )
    cmap.set_bad(color="#f2f2f2")

    im = ax.imshow(
        masked,
        aspect="auto",
        interpolation="nearest",
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
    )
    ax.set_title("Graph Benchmark Timing Heatmap (transparent=fast, red=slow)")

    ax.set_xticks(np.arange(cols))
    ax.set_xticklabels(systems, rotation=30, ha="right")
    ax.set_yticks(np.arange(rows))
    ax.set_yticklabels(queries)

    ax.set_xlabel("System")
    ax.set_ylabel("Query")

    # Subtle grid so cells remain readable for larger matrices.
    ax.set_xticks(np.arange(-0.5, cols, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, rows, 1), minor=True)
    ax.grid(which="minor", color="#ffffff", linestyle="-", linewidth=0.7, alpha=0.6)
    ax.tick_params(which="minor", bottom=False, left=False)

    # Colorbar labeled in ms (even though we plot log10(ms)).
    finite_ms = ms[np.isfinite(ms)]
    if finite_ms.size:
        min_ms = float(np.nanmin(finite_ms))
        max_ms = float(np.nanmax(finite_ms))
        ticks_ms = _nice_log_ticks(min_ms, max_ms)
        ticks_log = [math.log10(v) for v in ticks_ms if v > 0]
        cbar = fig.colorbar(im, ax=ax, fraction=0.032, pad=0.02, ticks=ticks_log)
        cbar.ax.set_yticklabels([f"{v:g} ms" for v in ticks_ms])
    else:
        fig.colorbar(im, ax=ax, fraction=0.032, pad=0.02)

    if annotate_extremes:
        # Overlay * on fastest (min) and x on slowest (max) for each query row.
        for r in range(rows):
            present: list[tuple[int, float]] = [
                (c, float(ms[r, c])) for c in range(cols) if np.isfinite(ms[r, c])
            ]
            if not present:
                continue
            min_c, _ = min(present, key=lambda t: t[1])
            max_c, _ = max(present, key=lambda t: t[1])

            ax.scatter(
                [min_c],
                [r],
                marker="*",
                s=140,
                facecolors="none",
                edgecolors="black",
                linewidths=1.2,
                zorder=3,
            )
            ax.scatter(
                [max_c],
                [r],
                marker="x",
                s=90,
                color="black",
                linewidths=2.0,
                zorder=3,
            )

        # Legend for markers (kept tiny; doesn't encode system colors).
        ax.scatter([], [], marker="*", s=90, facecolors="none", edgecolors="black", label="fastest")
        ax.scatter([], [], marker="x", s=70, color="black", label="slowest")
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=160)
    print(f"Wrote plot to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a heatmap from pytest-benchmark output files."
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path(__file__).resolve().parent / "benchmark_heatmap.png",
        help="Output path for the plot image.",
    )
    parser.add_argument(
        "--no-extremes",
        action="store_true",
        help="Disable fastest/slowest markers (* and x).",
    )
    args = parser.parse_args()

    results_dir = Path(__file__).resolve().parent
    files = _find_result_files(results_dir)
    if not files:
        raise SystemExit("No benchmark result files found in results directory.")

    parsed: dict[str, dict[str, float]] = {}
    for path in files:
        try:
            parsed[path.stem] = parse_benchmark_file(path)
        except ValueError:
            # Ignore non-benchmark files; keeps the script robust to extra artifacts.
            continue

    if not parsed:
        raise SystemExit("No benchmark tables detected in results directory.")

    systems = sorted(parsed.keys())
    all_queries = sorted(
        {q for results in parsed.values() for q in results.keys()},
        key=sort_query_key,
    )
    display_queries = [q.replace("test_benchmark_query", "q") for q in all_queries]

    values_by_query: list[list[float | None]] = []
    for query in all_queries:
        row: list[float | None] = []
        for system in systems:
            row.append(parsed[system].get(query))
        values_by_query.append(row)

    plot_heatmap(
        systems,
        display_queries,
        values_by_query,
        args.output,
        annotate_extremes=not args.no_extremes,
    )


if __name__ == "__main__":
    main()
