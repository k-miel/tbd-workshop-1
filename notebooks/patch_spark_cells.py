"""Patch the two PySpark cells in tbd_phase_2_26L.ipynb.

Fixes:
  1. SparkSession init: safer config for Windows/Jupyter, no memory_profiler conflict.
  2. Benchmark cell: remove cache leak, use psutil-based timing instead of memory_profiler.
"""
import json, pathlib, sys

NB = pathlib.Path(__file__).parent / "tbd_phase_2_26L.ipynb"
nb = json.loads(NB.read_text(encoding="utf-8"))

# ── New content for the two cells ─────────────────────────────────────────────

SPARK_INIT = [
    "import os\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "# Kill any stale SparkSession before creating a fresh one.\n",
    "# A leftover session from a previous run is the most common cause of hangs.\n",
    "try:\n",
    "    SparkSession.builder.getOrCreate().stop()\n",
    "except Exception:\n",
    "    pass\n",
    "\n",
    "# Windows / Jupyter compatibility tweaks:\n",
    "#  - bindAddress: prevents Spark from binding to a non-loopback address that\n",
    "#    Windows firewall blocks, causing the driver to wait forever.\n",
    "#  - ui.enabled=false: skips launching the Spark web UI server (saves ~5 s\n",
    "#    startup and avoids port-conflict hangs on Windows).\n",
    "#  - local[2]: use exactly 2 cores; local[*] on hyperthreaded machines spawns\n",
    "#    many executor threads that fight for the single JVM GC lock and can make\n",
    "#    small jobs slower, not faster.\n",
    "spark = (\n",
    "    SparkSession.builder\n",
    "    .appName(\"TBDPhase2LocalBenchmark\")\n",
    "    .master(\"local[2]\")\n",
    "    .config(\"spark.driver.memory\", \"4g\")\n",
    "    .config(\"spark.driver.bindAddress\", \"127.0.0.1\")\n",
    "    .config(\"spark.ui.enabled\", \"false\")\n",
    "    .config(\"spark.sql.shuffle.partitions\", \"4\")\n",
    "    .config(\"spark.sql.adaptive.enabled\", \"true\")\n",
    "    .config(\"spark.sql.adaptive.coalescePartitions.enabled\", \"true\")\n",
    "    .getOrCreate()\n",
    ")\n",
    "spark.sparkContext.setLogLevel(\"WARN\")\n",
    "print(\"Spark version:\", spark.version)\n",
    "print(\"Master:\", spark.sparkContext.master)\n",
    "print(\"Shuffle partitions:\", spark.conf.get(\"spark.sql.shuffle.partitions\"))\n",
]

SPARK_BENCH = [
    "import psutil, os, time, gc\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql.types import DateType\n",
    "\n",
    "print(\"PySpark local benchmark (master:\", spark.sparkContext.master, \")\")\n",
    "\n",
    "EVENTS_PARQUET_STR = str(EVENTS_PATH)\n",
    "DIMENSION_PARQUET_STR = str(DIMENSION_PATH)\n",
    "\n",
    "N_REPS_SPARK = 3  # fewer reps: Spark has high per-run overhead\n",
    "\n",
    "\n",
    "def run_benchmark_spark(fn, n_reps=N_REPS_SPARK):\n",
    "    \"\"\"Spark-safe benchmark: wall-clock timing only, no memory_profiler.\n",
    "\n",
    "    memory_profiler uses OS-level process monitoring that conflicts with\n",
    "    py4j's socket gateway, causing deadlocks. We measure RSS before/after\n",
    "    using psutil on the current process instead.\n",
    "    \"\"\"\n",
    "    proc = psutil.Process(os.getpid())\n",
    "    times = []\n",
    "    peak_mb = 0.0\n",
    "    for i in range(n_reps):\n",
    "        gc.collect()\n",
    "        rss_before = proc.memory_info().rss / 1e6\n",
    "        t0 = time.perf_counter()\n",
    "        fn()\n",
    "        elapsed = time.perf_counter() - t0\n",
    "        rss_after = proc.memory_info().rss / 1e6\n",
    "        times.append(elapsed)\n",
    "        peak_mb = max(peak_mb, rss_after)\n",
    "        print(f\"    rep {i+1}/{n_reps}: {elapsed:.2f}s  rss={rss_after:.0f}MB\")\n",
    "    times.sort()\n",
    "    median = times[len(times) // 2]\n",
    "    return round(median, 4), round(peak_mb, 1), times\n",
    "\n",
    "\n",
    "# ── Q1 ────────────────────────────────────────────────────────────────────────\n",
    "\n",
    "def spark_q1():\n",
    "    \"\"\"No .cache() inside: each rep re-reads and re-computes to get stable timing.\"\"\"\n",
    "    df = spark.read.parquet(EVENTS_PARQUET_STR)\n",
    "    result = (\n",
    "        df.filter(\n",
    "            (F.col(\"event_date\") >= F.lit(\"2026-01-01\").cast(DateType())) &\n",
    "            (F.col(\"event_date\") <= F.lit(\"2026-01-31\").cast(DateType())) &\n",
    "            (F.col(\"country\") == \"PL\") &\n",
    "            F.col(\"revenue\").isNotNull()\n",
    "        )\n",
    "        .groupBy(\"event_date\")\n",
    "        .agg(\n",
    "            F.sum(\"revenue\").alias(\"total_revenue\"),\n",
    "            F.avg(\"revenue\").alias(\"avg_revenue\"),\n",
    "            F.count(\"*\").alias(\"order_count\"),\n",
    "        )\n",
    "        .orderBy(\"event_date\")\n",
    "    )\n",
    "    # .collect() materialises lazily — no cache needed for timing\n",
    "    return result.collect()\n",
    "\n",
    "\n",
    "print(\"Running Q1 warmup...\")\n",
    "_r1 = spark_q1()\n",
    "q1_check_spark = round(sum(row[\"total_revenue\"] for row in _r1), 2)\n",
    "print(f\"Q1 check (Spark): {q1_check_spark}  rows={len(_r1)}\")\n",
    "\n",
    "med, mem, _ = run_benchmark_spark(spark_q1)\n",
    "record(\"pyspark\", \"local[2]\", \"Q1_daily_revenue_filter\", \"parquet\", \"default\",\n",
    "       N_ROWS, med, mem, EVENTS_SIZE_MB, q1_check_spark,\n",
    "       f\"spark {spark.version} local[2] shuffle_partitions=4 no-cache\")\n",
    "print(f\"Q1 median={med:.3f}s  peak_rss={mem:.0f}MB\")\n",
    "\n",
    "# ── Q2 ────────────────────────────────────────────────────────────────────────\n",
    "\n",
    "def spark_q2():\n",
    "    orders = spark.read.parquet(EVENTS_PARQUET_STR)\n",
    "    products = spark.read.parquet(DIMENSION_PARQUET_STR)\n",
    "    result = (\n",
    "        orders.filter(F.col(\"revenue\").isNotNull())\n",
    "        .join(F.broadcast(products), on=\"product_id\", how=\"inner\")\n",
    "        .groupBy(\"category\")\n",
    "        .agg(\n",
    "            F.sum(\"revenue\").alias(\"total_revenue\"),\n",
    "            F.avg(\"revenue\").alias(\"avg_order_value\"),\n",
    "            F.count(\"*\").alias(\"order_count\"),\n",
    "            F.sum(\"quantity\").alias(\"total_quantity\"),\n",
    "        )\n",
    "        .orderBy(F.desc(\"total_revenue\"))\n",
    "    )\n",
    "    return result.collect()\n",
    "\n",
    "\n",
    "print(\"\\nRunning Q2 warmup...\")\n",
    "_r2 = spark_q2()\n",
    "q2_check_spark = round(sum(row[\"total_revenue\"] for row in _r2), 2)\n",
    "print(f\"Q2 check (Spark): {q2_check_spark}  rows={len(_r2)}\")\n",
    "\n",
    "med, mem, _ = run_benchmark_spark(spark_q2)\n",
    "record(\"pyspark\", \"local[2]\", \"Q2_category_revenue_join\", \"parquet\", \"default\",\n",
    "       N_ROWS, med, mem, EVENTS_SIZE_MB + DIMENSION_SIZE_MB, q2_check_spark,\n",
    "       f\"spark {spark.version} local[2] broadcast_join shuffle_partitions=4 no-cache\")\n",
    "print(f\"Q2 median={med:.3f}s  peak_rss={mem:.0f}MB\")\n",
    "\n",
    "# ── Q3 ────────────────────────────────────────────────────────────────────────\n",
    "\n",
    "def spark_q3():\n",
    "    df = spark.read.parquet(EVENTS_PARQUET_STR)\n",
    "    result = (\n",
    "        df.filter(F.col(\"revenue\").isNotNull())\n",
    "        .groupBy(\"customer_id\")\n",
    "        .agg(\n",
    "            F.sum(\"revenue\").alias(\"total_revenue\"),\n",
    "            F.count(\"*\").alias(\"order_count\"),\n",
    "            F.avg(\"quantity\").alias(\"avg_quantity\"),\n",
    "        )\n",
    "        .orderBy(F.desc(\"total_revenue\"))\n",
    "        .limit(50)\n",
    "    )\n",
    "    return result.collect()\n",
    "\n",
    "\n",
    "print(\"\\nRunning Q3 warmup...\")\n",
    "_r3 = spark_q3()\n",
    "q3_check_spark = round(sum(row[\"total_revenue\"] for row in _r3), 2)\n",
    "print(f\"Q3 check (Spark): {q3_check_spark}  rows={len(_r3)}\")\n",
    "\n",
    "med, mem, _ = run_benchmark_spark(spark_q3)\n",
    "record(\"pyspark\", \"local[2]\", \"Q3_top50_customers\", \"parquet\", \"default\",\n",
    "       N_ROWS, med, mem, EVENTS_SIZE_MB, q3_check_spark,\n",
    "       f\"spark {spark.version} local[2] shuffle_partitions=4 no-cache\")\n",
    "print(f\"Q3 median={med:.3f}s  peak_rss={mem:.0f}MB\")\n",
    "\n",
    "print(\"\\nPySpark local benchmarks complete.\")\n",
]

# ── Locate and patch the two cells ────────────────────────────────────────────

def src(cell):
    return "".join(cell.get("source", []))

INIT_MARKER = "TBDPhase2LocalBenchmark"
BENCH_MARKER = "PySpark local benchmark (master:"

patched = {"init": False, "bench": False}

for cell in nb["cells"]:
    if cell.get("cell_type") != "code":
        continue
    s = src(cell)
    if INIT_MARKER in s and "getOrCreate" in s and not patched["init"]:
        cell["source"] = SPARK_INIT
        cell["outputs"] = []
        cell["execution_count"] = None
        patched["init"] = True
        print("Patched: SparkSession init cell")
    elif BENCH_MARKER in s and not patched["bench"]:
        cell["source"] = SPARK_BENCH
        cell["outputs"] = []
        cell["execution_count"] = None
        patched["bench"] = True
        print("Patched: Spark benchmark cell")

if not patched["init"]:
    print("WARNING: SparkSession init cell NOT found — check marker string")
if not patched["bench"]:
    print("WARNING: Spark benchmark cell NOT found — check marker string")

NB.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print("Notebook saved.")
