import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

# ---- Config: map "case name" -> locust stats history csv file ----
CASES = {
    "local": "local_stats_history.csv",
    "vm_scaleset_shutdown": "scaleset_shutdown_stats_history.csv",
    "webapp_autoscale": "webapp_autoscale_stats_history.csv",
    "function_autoscale": "history_function_stats_history.csv",
}

OUT_PNG = "locust_throughput.png"


def _pick_col(cols, candidates):
    """Pick the first matching column name from candidates (case-insensitive)."""
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def load_rps(path: str):
    df = pd.read_csv(path)

    # Common locust columns differ slightly by version; be robust.
    col_time = _pick_col(df.columns, ["Timestamp", "timestamp", "Time", "time"])
    col_rps = _pick_col(df.columns, ["Requests/s", "Requests/s ", "requests/s", "req/s", "RPS", "rps"])
    col_name = _pick_col(df.columns, ["Name", "name"])
    col_type = _pick_col(df.columns, ["Type", "type", "Method", "method"])

    if col_time is None or col_rps is None:
        raise ValueError(
            f"Cannot find required columns in {path}. "
            f"Found columns: {list(df.columns)}"
        )

    # Try to keep only the aggregated row if present.
    if col_name is not None:
        # Different versions use "Aggregated", "Total", or blank name for totals.
        mask = df[col_name].astype(str).str.lower().isin(["aggregated", "total", "none", ""])
        if mask.any():
            df = df.loc[mask].copy()

    # Some versions include per-type rows; keep the one that looks like total.
    if col_type is not None:
        # Prefer "Aggregated"/"None" rows if they exist.
        t = df[col_type].astype(str).str.lower()
        mask = t.isin(["aggregated", "none", ""])
        if mask.any():
            df = df.loc[mask].copy()

    # Parse time: locust history typically uses unix seconds.
    ts = df[col_time]
    # If numeric -> treat as epoch seconds; if not, let pandas parse.
    if pd.api.types.is_numeric_dtype(ts):
        t = pd.to_datetime(ts, unit="s", utc=True).dt.tz_convert(None)
    else:
        t = pd.to_datetime(ts, utc=True, errors="coerce").dt.tz_convert(None)
        if t.isna().all():
            raise ValueError(f"Timestamp parsing failed for {path}: sample={ts.head(3).tolist()}")

    rps = pd.to_numeric(df[col_rps], errors="coerce")
    out = pd.DataFrame({"time": t, "rps": rps}).dropna().sort_values("time")

    # Normalize to seconds since start (easier to compare across runs)
    out["t_sec"] = (out["time"] - out["time"].iloc[0]).dt.total_seconds()
    return out[["t_sec", "rps"]]


def main():
    missing = [f for f in CASES.values() if not os.path.exists(f)]
    if missing:
        print("❌ Missing required CSV files in current directory:")
        for m in missing:
            print("  -", m)
        print("\nCurrent directory:", os.getcwd())
        print("Files here:", sorted(os.listdir("."))[:50], "...")
        sys.exit(1)

    series = {}
    for label, fname in CASES.items():
        try:
            series[label] = load_rps(fname)
        except Exception as e:
            print(f"❌ Failed to parse {fname}: {e}")
            raise

    plt.figure()
    for label, df in series.items():
        plt.plot(df["t_sec"], df["rps"], label=label)

    plt.xlabel("time since start (s)")
    plt.ylabel("successful requests / second")
    plt.title("Locust throughput comparison (RPS)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=200)

    print(f"✅ Saved plot: {OUT_PNG}")


if __name__ == "__main__":
    main()
