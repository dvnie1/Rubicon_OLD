#!/usr/bin/env bash

rm -rf build
source ext/SIMDOperators/tools/tslgen/.venv/bin/activate
cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DPython3_EXECUTABLE=$(which python) \
    -DCMAKE_EXE_LINKER_FLAGS="-Wl,--copy-dt-needed-entries"
cmake --build build -j$(nproc)