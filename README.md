# Rubicon

**Less Moves, More Queries: Exploiting Access Redundancy in CXL-enabled Database Systems**

Rubicon is a disaggregated database system prototype that reduces redundant data movement in read-heavy analytical workloads over far-memory interfaces such as CXL. It batches incoming queries, groups them by overlapping column access patterns (*pipeline grouping*), and selectively copies shared hot data from far-memory (CXL) to near-memory (DRAM) using a cost model that weighs one-time copy overhead against reuse savings.

> Paper under review at PVLDB 19, 2026

---

## Repository Structure

```
Rubicon/
├── computeUnit/          # Compute Unit (CU): executes grouped query plans via SIMD operators
├── grouper/              # Grouper: batching, pipeline grouping, and cost-model decisions
├── experiments/          # Python experiment drivers and workload configs
│   ├── configs/          # YAML workload definitions (Morning Dashboard, etc.)
│   └── utils/            # Shared utilities (TCP client, query definitions, arrival models)
├── data/                 # SSB data generation and binary conversion
├── ext/SIMDOperators     # Git submodule: SIMD-based operator library (includes TSL generator)
├── pg_potential_experiment/  # Standalone microbenchmark (Figure 1 in the paper)
├── proto/                # Protobuf message definitions
├── CMakeLists.txt        # Top-level CMake build
└── generate_python_proto.sh  # Script to generate Python protobuf bindings
```

---

## Prerequisites

### Operating System

Linux (tested on Ubuntu). NUMA support is required.

### System Packages

```bash
sudo pacman -Syu --noconfirm --needed \
  base-devel \
  cmake \
  git \
  numactl \
  protobuf \
  python \
  python-pip \
  graphviz \
  uv
```

**Minimum versions:** CMake ≥ 3.20, C++20-capable compiler (GCC ≥ 11 or Clang ≥ 14), protobuf (compatible with `protobuf==4.21.12` for Python bindings).

### Python (for the TSL Generator inside SIMDOperators)

The `ext/SIMDOperators` submodule includes the [TSL (Template SIMD Library)](https://github.com/db-tu-dresden/TSL) generator as a nested submodule (`tools/tslgen`). The CMake build invokes this generator, which requires Python 3 and several packages. Install the TSL generator's dependencies:

```bash
cd ext/SIMDOperators/tools/tslgen
uv venv
uv pip install -r requirements.txt
cd -
```

> **Note:** If you cannot install `graphviz-dev` (needed for `pygraphviz`), remove `pygraphviz` from that `requirements.txt` and pass `--no-draw-test-dependencies` to the generator if invoked manually.

### Python (for Data Generation)

Both `data/` and `experiments/` directories use `uv` for dependency management (installed system-wide via pacman).
Dependencies will be installed automatically by `uv run` based on the respective `pyproject.toml` files.

**Required Python version:** ≥ 3.12, < 3.13

### Hardware (for Paper Reproduction)

The paper evaluates on:

- **Compute Unit server:** Intel Xeon 6741P (48 cores, up to 3.8 GHz), 512 GB DDR5, SMART Modular Technologies CXA-8F2W CXL 2.0 Type 3 add-in card (512 GB), configured as a headless NUMA node.
- **Grouper server:** AMD Ryzen Threadripper PRO 7955WX (16 cores), 256 GB DDR5. Connected via 1 GbE Ethernet.

For testing without CXL hardware, the NUMA evaluation (Section 7 of the paper) shows the approach also works on a dual-socket server where remote-socket DRAM serves as far-memory.

Both the Grouper and the Compute Unit can run on the same machine for functional testing.

---

## Step-by-Step Reproduction

### 1. Clone the Repository (with Submodules)

```bash
git clone --recurse-submodules https://github.com/AndreasGeyerTUD/Rubicon.git
cd Rubicon
```

If you already cloned without `--recurse-submodules`:

```bash
git submodule update --init --recursive
```

### 2. Generate SSB Benchmark Data

```bash
cd data
```

Edit `setup.sh` to set the desired scale factors. By default it generates SF 100. For the paper's experiments, use SF 100 (or SF 50 for the NUMA evaluation):

```bash
# In setup.sh, change SCALE_FACTORS to your desired value, e.g.:
# SCALE_FACTORS=(100)
```

Then run:

```bash
export CC="cc -std=gnu89 -Wno-error=incompatible-pointer-types"
bash setup.sh
cd ..
```

This clones the [ssb-kit](https://github.com/gregrahn/ssb-kit) generator, builds it, generates CSV data, converts it to binary column format using `uv run convert_to_binary.py`, and cleans up intermediate files. The resulting binary data will be in `data/binary/sf<N>/`.

### 3. Build the C++ Components

```bash
source ext/SIMDOperators/tools/tslgen/.venv/bin/activate
cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DPython3_EXECUTABLE=$(which python) \
    -DCMAKE_EXE_LINKER_FLAGS="-Wl,--copy-dt-needed-entries"
cmake --build build -j$(nproc)
```

This builds three executables in `build/bin/`:

| Executable | Description |
|---|---|
| `grouper` | The Grouper server (batching, grouping, cost model) |
| `computeUnit` | The Compute Unit (query execution engine) |
| `pg_potential_bench` | Standalone microbenchmark for pipeline grouping potential |

**Optional CMake flags:**

- `-DLOG_LEVEL=LOG_DEBUG1` — Enable verbose logging (default: `LOG_INFO`)
- `-DGROUPER_VERBOSE=ON` — Enable verbose grouper output
- `-DGROUPER_PROFILING=ON` — Enable CSV timing for grouping analysis

### 4. Generate Python Protobuf Bindings

The experiment scripts communicate with the Grouper using Protobuf over TCP. Generate the Python bindings:

```bash
bash generate_python_proto.sh
```

This creates `experiments/proto/*_pb2.py` files.

### 5. Start the System

The system consists of three components that must be started **in this order**:

**Terminal 1 — Start the Grouper:**

```bash
./build/bin/grouper -port 23232
```

**Terminal 2 — Start the Compute Unit:**

```bash
./build/bin/computeUnit \
    -ip 127.0.0.1 \
    -port 23232 \
    -path data/binary/sf100 \
    -basedata bin \
    -worker 48 \
    -node 0 \
    -cxl_node 1
```

| Flag | Description |
|---|---|
| `-ip` | IP address of the Grouper (use `127.0.0.1` if on the same machine) |
| `-port` | Port the Grouper is listening on |
| `-path` | Path to the binary SSB data directory |
| `-basedata` | Data format: `bin` (binary) or `csv` |
| `-worker` | Number of worker threads (default: logical cores per NUMA node) |
| `-node` | NUMA node affinity for compute threads (`-1` = all nodes) |
| `-cxl_node` | NUMA node where base data resides, e.g., the CXL device node (`-1` = any) |

> **Tip:** Use `numactl -H` or `lscpu` to identify which NUMA node corresponds to your CXL device or remote socket.

**Terminal 3 — Run an experiment** (see below).

---

## Running the Experiments

All experiment scripts are in the `experiments/` directory and use `uv` for dependency management. Run them with `uv run` from within the `experiments/` directory.

### Experiment 1: Grouping Overhead (Paper Section 5.2, Figure 4)

Measures the overhead introduced by the pipeline grouping algorithm at varying concurrency levels.

```bash
cd experiments
uv run grouping_overhead_experiment.py \
    -ip 127.0.0.1 \
    -port 23232 \
    --scale-factor 100 \
    --workers-per-cu 48 \
    --n-values "10,25,50,100,150,200,300,400,500,650,800,1000" \
    --iterations 10
```

### Experiment 2: Concurrent Query Execution (Paper Section 5.3, Figure 5 & Table 4)

Compares Naïve vs. Pipeline Grouping execution at different concurrency levels.

```bash
cd experiments
uv run concurrent_query_execution_experiment.py \
    -ip 127.0.0.1 \
    -port 23232 \
    -q "q_1_1,q_2_1,q_3_1,q_4_2" \
    --scale-factor 100 \
    --workers-per-cu 48 \
    --n-values "5,10,15,20,25,30,40,50,60,80,100" \
    --window-size 100 \
    --iterations 10 \
    --out results/concurrent_experiment
```

The `-q` flag specifies which SSB queries to evaluate (CSV). Available queries: `q_1_1` through `q_1_3`, `q_2_1` through `q_2_3`, `q_3_1` through `q_3_4`, `q_4_1` through `q_4_3`.

### Experiment 3: Workload Evaluation (Paper Section 6, Table 5)

Runs production-inspired workload profiles defined as YAML configs. To run the Morning Dashboard workload:

```bash
cd experiments
uv run workload_experiment.py \
    -config configs/workload_morning_dashboard.yaml \
    -ip 127.0.0.1 \
    -port 23232 \
    --window-size 2000 \
    --threshold 10
```

Available workload configs in `experiments/configs/`:

| Config File | Paper Section |
|---|---|
| `workload_morning_dashboard.yaml` | Section 6.1 |
| `workload_adhoc_exploration.yaml` | Section 6.2 |
| `workload_peak_mixed.yaml` | Section 6.3 |
| `workload_microbursts.yaml` | Section 6.4 |
| `workload_stress_test.yaml` | Section 6.5 |

To reproduce Table 5, run each workload config with multiple `--window-size` values (100, 500, 1000, 2000, 3000 ms). Each experiment runs the baseline before it runs the workload.

Additional flags:

| Flag | Description | Default |
|---|---|---|
| `--window-size` | Batching window in ms | 100 |
| `--threshold` | Overhead threshold for grouping | 10 |
| `--max-overhead` | Maximum overhead ratio (τ_ρ) | 2.0 |
| `-dry-run` | Validate config without executing | — |
| `-o` | Override output CSV path | — |

### Standalone: Pipeline Grouping Potential (Paper Figure 1)

This microbenchmark does **not** require the Grouper or experiment scripts. It runs directly on a NUMA system:

```bash
./build/bin/pg_potential_bench <cxl_node> <dram_node> [reps] [chunk_mib] [csv_file] [--scalar]
```

Example (CXL on NUMA node 1, DRAM on node 0):

```bash
./build/bin/pg_potential_bench 1 0 5 64 results.csv
```

> **Note:** This benchmark allocates 48 × 5 GiB on the CXL/remote node plus 5 GiB on the local node. Ensure sufficient memory.

---

## NUMA-Only Evaluation (Without CXL Hardware)

The cost model generalizes to any near-/far-memory asymmetry (Paper Section 7). On a dual-socket server, use remote-socket DRAM as far-memory:

```bash
./build/bin/computeUnit \
    -ip 127.0.0.1 \
    -port 23232 \
    -path data/binary/sf50 \
    -basedata bin \
    -worker 24 \
    -node 0 \
    -cxl_node 1
```

Here `-node 0` pins compute to socket 0, and `-cxl_node 1` places base data on socket 1's DRAM (the "far-memory" tier).

---

## Known Limitations

- **CXL thermal throttling:** The CXL device used in the paper is thermally unstable at high concurrency. Experiments are capped at 100 concurrent queries. See Section 5.1 of the paper for details.
- **Single Compute Unit:** The current prototype supports only a single CU. Multi-CU execution is architecturally supported but not evaluated.
- **No cross-batch caching:** Each query batch operates independently; copied data is released after batch completion. Cross-batch caching is discussed as future work (Section 3.3).

---

## License

This project is licensed under the [GPL-3.0 License](LICENSE).

## Acknowledgments

Funded by the Deutsche Forschungsgemeinschaft (DFG, German Research Foundation) through Reinhart Koselleck Project: Serverless Data Management Primitives for Software-defined Composable Systems — project number 450276976.