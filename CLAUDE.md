# CLAUDE.md — TBD Phase 2 26L (Grupa 2: E-commerce orders)

Postęp prac nad notebookiem `notebooks/tbd_phase_2_26L.ipynb` — benchmark Pandas 3.0 /
Polars / DuckDB / PySpark. Plik służy jako pamięć projektu: co zrobiono i co zostało.

## Decyzje (zatwierdzone przez użytkownika)
- **Wariant: Grupa 2 — E-commerce orders.** Wymagana cecha: produkty + wartości zamówień.
  Stress: **join + agregacja po kategorii.**
- **Główny rozmiar `N_ROWS = 10_000_000`** (`SCALE = "medium"`).
- **Dataproc: pełne uruchomienie** na klastrze GCP (Task 5).
- **Wykonanie notebooka end-to-end** i osadzenie wyników w komórkach.

## Środowisko (zweryfikowane)
- Python 3.12.10, pandas 3.0.3, polars 1.41.2, duckdb 1.5.3, pyspark 4.1.2, pyarrow 24,
  faker, psutil, memory_profiler — zainstalowane.
- Java 17 (`JAVA_HOME` ustawione) — wymagane przez PySpark 4.x.
- 32 GiB RAM / 16 rdzeni — 10M wierszy mieści się komfortowo.
- `nbconvert` 7.17.1 — doinstalowane (do wykonania notebooka).
- **gcloud SDK: NIE zainstalowane** (brak w Bash i PowerShell) — blokada dla Task 5.
- **Windows + PySpark:** odczyt Parquet działa; **zapis** Parquet z lokalnego Sparka pada
  (`HADOOP_HOME`/winutils nieustawione). Dlatego wszystkie zapytania Spark są **tylko-odczyt**.

## Schemat danych (zaprojektowany)
Tabela `events` (12 kolumn): `order_id` (unikalny), `customer_id` (high-card, skośny),
`order_ts`, `order_date`, `product_id` (FK, skośny), `product_category` (8 kategorii,
deterministycznie z `product_id`), `country`, `device`, `payment_method` (~3% NULL),
`order_status` (completed/returned/cancelled), `order_value` (lognormal EUR), `quantity`.
Wymiar `products`: `product_id` → `brand`, `supplier_country`, `unit_price`
(te atrybuty są TYLKO w wymiarze → Q1 wymaga joina).

Trzy zapytania:
- **Q1** — join events↔products + agregacja przychodu po `supplier_country` (wymóg wariantu).
- **Q2** — high-cardinality group-by po `customer_id` + top-20 (filtr `completed`).
- **Q3** — selektywny filtr (zakres dat + `product_category='electronics'`) + agregacja dzienna;
  używany ponownie w Task 2.5 (layout/pruning). Stałe: 2026-02-01..2026-02-14, kategoria electronics.

## ZROBIONE (komórki edytowane w notebooku)
- [x] `cell-5` — instalacja pip wykomentowana (no-op przy re-runie nbconvert).
- [x] `cell-11` — `SCALE="medium"` (10M), ścieżki wyjściowe.
- [x] `cell-13` — generator E-commerce (`generate_base_events` bez kolumn list/tagów,
      `customize_for_variant`, `generate_dimension_table`).
- [x] `cell-14` — generacja + zapis (default parquet, partycjonowany po `order_date`,
      **optimized** sort+`row_group_size=100_000`), manifest.
- [x] `cell-16` — sanity checks (schema, nulls, kardynalności, rozkłady, skew, rozmiary na dysku).
- [x] `cell-18` — harness benchmarku: `REPEATS=3`, `benchmark_query()`, `check_q1/q2/q3()`,
      `results_df()`, pomiar mediany czasu + peak RSS (n/a dla Sparka — JVM).
- [x] `cell-20` — specyfikacje Q1/Q2/Q3 + hipotezy (renderowane jako Markdown).
- [x] `cell-22` — sesja Spark local (`local[*]`, 6g, shuffle=16, `build_local_spark()`).
- [x] `cell-23` — Pandas Q1-Q3 dla obu backendów (numpy + pyarrow), wydruk dtypes.
- [x] `cell-24` — Polars Q1-Q3 w trybach eager / lazy / streaming.
- [x] `cell-25` — DuckDB Q1-Q3 (SQL bezpośrednio na `read_parquet`).
- [x] `cell-26` — PySpark local Q1-Q3 (tylko-odczyt, `measure_memory=False`).
- [x] nowa komórka po `cell-26` (id `9ab0b00e`) — tabela wyników Task 2 + sprawdzenie
      równoważności wyników między silnikami.
- [x] Walidacja logiki generatora i Q1/Q2/Q3 na 50k wierszy — OK.
- [x] Walidacja: Spark czyta Parquet OK, zapis pada (winutils) — potwierdzone.

## STATUS: część lokalna UKOŃCZONA ✅
Notebook został w całości wypełniony i **wykonany end-to-end na 10M wierszy (exit 0, zero błędów
komórek)**. Wszystkie silniki zgadzają się co do wyników (Q1/Q2/Q3), tabele, wykresy i 8 odpowiedzi
końcowych zawierają realne liczby. Pozostał JEDYNIE realny przebieg Dataproc (Task 5), który wymaga
gcloud + interaktywnej autoryzacji + klastra (sekcja 7). Komórka Dataproc renderuje się jako "pending"
dopóki nie ma pliku `data/phase2_26L/group_02/dataproc_results.json`.

Wybrane wyniki 10M (mediana s): Q1 — DuckDB 0.083, Polars-lazy 0.20, Spark-local 1.07, Pandas-numpy 1.64.
Task 2.5 Q3: parquet-default 0.041, optimized 0.010, CSV 0.303. Skalowanie DuckDB Q1: 1→0.40, 16→0.08.

UWAGA techniczna (na przyszłość): edycja komórek przez NotebookEdit po `insert` przenumerowuje
`cell-N` — kolejne `replace` mogą trafić w złą komórkę. Tu naprawione przez rebuild JSON.

## TODO (pozostałe)

### 1. (DONE) Poprawka komórki równoważności
- [ ] `cell 9ab0b00e`: zamiast porównania całych stringów `result_check`, **parsować pola**
      i porównywać liczniki (`orders`, `days`, `groups`, `top_customer`) dokładnie, a `rev`
      z tolerancją (różny porządek sumowania float między silnikami może dać ±1 EUR).

### 2. Task 2.5 — format pliku i layout (`cell-28`)
- [ ] Wybrać Q3. Benchmark tego samego zapytania na: (a) default Parquet, (b) optimized
      Parquet (sort + `row_group_size=100k`), (c) **CSV** (negatywny baseline, płaski — tylko
      kolumny Q3 → `CSV_EVENTS_PATH`).
- [ ] Dowód pruningu: `EXPLAIN ANALYZE` w DuckDB (liczba zeskanowanych wierszy default vs
      optimized) i/lub `.explain()` w Polars (predicate/projection pushdown).
- [ ] Raport: format, rozmiar wejścia, liczba plików, czas, peak mem, checksum, wyjaśnienie
      DLACZEGO layout pomaga.

### 3. Task 3.1 — tryby wykonania Polars (`cell-31`)
- [ ] Zapytanie z **dużym wyjściem** (filtr + projekcja, wiele wierszy), tryby: eager,
      lazy `collect()`, `collect(engine="streaming")`, `sink_parquet(...)`.
- [ ] Zapis: czas, peak mem, liczba wierszy wyjścia, rozmiar wyjścia. Komentarz o ograniczeniu
      pomiaru pamięci w jednym kernelu (gc.collect przed każdym; idealnie osobny proces).

### 4. Task 3.2 / 3.3 — proza (`cell-33`, `cell-35`)
- [ ] `cell-33`: scenariusz ograniczenia Polars vs Spark + dowód z naszych pomiarów
      (sufit pamięci jednego węzła / wyjście ≈ wejście / skośność).
- [ ] `cell-35`: konkretna granica decyzyjna single-node → Spark, oparta na pomiarach.

### 5. Task 4 — skalowalność wątków/rdzeni (`cell-37`)
- [ ] DuckDB: `SET threads TO 1/2/N`, jedno zapytanie, mediana czasu.
- [ ] PySpark: `local[1]`/`local[2]`/`local[*]` (trzeba stop+rebuild sesji), to samo zapytanie.
- [ ] Polars: nota, że pula wątków wymaga restartu procesu (opcjonalnie osobny run).
- [ ] Wyjaśnić, dlaczego skalowanie jest sub-liniowe (IO-bound, shuffle, stały narzut).

### 6. Wykresy + higiena (`cell` nowa po `cell-37`, `.gitignore`)
- [ ] Dodać komórkę z wykresami (matplotlib/seaborn): czas wg silnika/zapytania, peak mem.
- [ ] `.gitignore`: dopisać `data/` (lub `data/phase2_26L/`), `*.parquet`, `*.csv`, `*.jsonl`
      — żeby NIE commitować wygenerowanych danych.

### 7. Task 5 — Spark na Dataproc (`cell-39`) — WYMAGA PREREKVIZYTÓW
- [ ] Zainstalować Google Cloud SDK (gcloud).
- [ ] `gcloud auth login` + `gcloud auth application-default login` (INTERAKTYWNE — potrzebny
      użytkownik), projekt `tbd-2026l-321362`.
- [ ] Upewnić się, że klaster Dataproc `tbd-cluster` działa (`terraform apply` — KOSZT GCP).
      Buckety: `tbd-2026l-321362-data`, `tbd-2026l-321362-code`.
- [ ] Wgrać Parquet do `gs://tbd-2026l-321362-data/phase2_26L/group_02/`.
- [ ] Napisać mały skrypt PySpark (wzór: `modules/data-pipeline/resources/spark-job.py`),
      `gcloud dataproc jobs submit pyspark ... --cluster tbd-cluster --region <region>`.
- [ ] Zebrać czasy, porównać local vs Dataproc, wyjaśnić różnice (partycje, shuffle, narzut
      schedulera). Uwaga: klaster ma `pandas<2`, więc uruchamiamy tylko zapytania Spark.
      BEZ hardkodowania sekretów.

### 8. Odpowiedzi końcowe (`cell-42`..`cell-49`)
- [ ] Wypełnić `FINAL_ANSWER_1..8` na podstawie zmierzonych wyników.

### 9. Wykonanie end-to-end
- [ ] `jupyter nbconvert --to notebook --execute --inplace notebooks/tbd_phase_2_26L.ipynb`
      (długi timeout). Naprawić ewentualne błędy komórek, powtórzyć aż czysto.
- [ ] Krok Dataproc uruchomić po autoryzacji gcloud i postawieniu klastra.
- [ ] `git status` — sprawdzić, że żadne wygenerowane dane nie są w stage.

## Uwagi / ryzyka
- Pełny run 10M × 5 silników × 3 zapytania × 3 powt. + skalowalność jest długi (kilka–
  kilkanaście minut). To normalne.
- Pomiar peak mem jest miarodajny tylko dla silników in-process (Pandas/Polars/DuckDB),
  i tylko orientacyjny we wspólnym kernelu.
- Plan szczegółowy: `C:\Users\kacpe\.claude\plans\przeanalizuj-plik-notebooks-tbd-phase-2-wiggly-shore.md`.
