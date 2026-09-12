# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Render the captured, anonymized benchmark timelines with matplotlib."""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

ROOT = Path(__file__).resolve().parent
DATA = json.loads((ROOT / "io-overlap-timeline.json").read_text())
GOLD, BLUE, RED, GRAY = "#e7b928", "#46a9ee", "#d87770", "#343944"
plt.rcParams.update({"font.size": 11, "figure.facecolor": "#f8fafc"})


def style(ax):
    ax.set_facecolor("#20242b")
    ax.grid(axis="x", alpha=0.15)
    ax.set_axisbelow(True)
    for spine in ax.spines.values():
        spine.set_visible(False)


fig, axes = plt.subplots(2, 2, figsize=(15, 7), height_ratios=[4, 1], sharex="col")
limit = max(d["elapsed_ms"] for d in DATA["native"].values()) * 1.04
for col, label in enumerate(["current", "yield"]):
    d = DATA["native"][label]
    assert len(d["workers"]) == 8
    ax = axes[0, col]
    style(ax)
    for i, spans in enumerate(d["workers"]):
        for state, color in [("Blocked", GRAY), ("Preempted", RED), ("Runnable", RED), ("Running", GOLD)]:
            ax.broken_barh([(a, b-a) for a, b, s in spans if s == state], (i-0.35, 0.7), facecolors=color)
    ax.set(yticks=range(8), yticklabels=[f"Worker {i+1}" for i in range(8)], ylim=(7.6, -0.6))
    ax.set_title(f'{"Before" if col == 0 else "After yield"} · {d["elapsed_ms"]:.1f} ms', loc="left", fontweight="bold")
    io = axes[1, col]
    style(io)
    edges = np.arange(0, limit+0.5, 0.5)
    cpu = np.zeros(len(edges)-1)
    for spans in d["io"]:
        for a, b, state in spans:
            if state == "Running":
                cpu += np.maximum(0, np.minimum(edges[1:], b)-np.maximum(edges[:-1], a))/0.5
    io.stairs(cpu, edges, fill=True, color=BLUE)
    io.set(xlabel="Time since query start (ms)", ylabel="I/O pool\nCPU cores", xlim=(0, limit), ylim=(0, 8), yticks=[0, 4, 8])
    for a in [ax, io]:
        a.axvline(d["elapsed_ms"], color="#a7b3c4", lw=1, ls="--")
fig.suptitle("Eight scan partitions: actual CPU execution and waiting", x=0.06, ha="left", fontsize=18, fontweight="bold")
fig.legend(handles=[Patch(color=GOLD, label="Running on CPU"), Patch(color=RED, label="Runnable / preempted"), Patch(color=GRAY, label="Blocked"), Patch(color=BLUE, label="Local-file pool CPU (sum)")], loc="lower center", ncol=4, frameon=False)
fig.subplots_adjust(left=0.08, right=0.98, top=0.88, bottom=0.14, hspace=0.1, wspace=0.18)
fig.savefig(ROOT / "io-overlap-native.png", dpi=180)
plt.close(fig)

fig, axes = plt.subplots(2, 2, figsize=(15, 6.4))
limit = max(d["elapsed_ms"] for d in DATA["runtime"].values()) * 1.02
for col, label in enumerate(["current", "yield"]):
    d = DATA["runtime"][label]
    for row in range(2):
        ax = axes[row, col]
        style(ax)
        ax.broken_barh([(a, b-a) for a, b in d["active"]], (0.65, 0.6), facecolors=GOLD)
        ax.broken_barh([(a, b-a) for a, b in d["reads"]], (-0.25, 0.6), facecolors=BLUE)
        ax.set(yticks=[0.05, 0.95], yticklabels=["Read pending", "Worker active"], ylim=(-0.6, 1.6), xlabel="Time since query start (ms)", xlim=(0, limit) if row == 0 else (30, 110))
        if row == 0:
            ax.axvline(d["elapsed_ms"], color="#a7b3c4", ls="--", lw=1)
            ax.set_title(f'{"Before" if col == 0 else "After yield"} · whole query, {d["elapsed_ms"]:.1f} ms', loc="left", fontweight="bold")
        else:
            ax.set_title("Same 80 ms window, enlarged", loc="left")
fig.suptitle("One scan partition: queued reads become overlapping reads", x=0.08, ha="left", fontsize=18, fontweight="bold")
fig.text(0.08, 0.025, "Worker active = at least one Tokio worker unparked; read pending = reader future lifetime. These are not OS CPU or disk-time measurements.", fontsize=10)
fig.subplots_adjust(left=0.10, right=0.98, top=0.87, bottom=0.13, hspace=0.52, wspace=0.22)
fig.savefig(ROOT / "io-overlap-runtime.png", dpi=180)
plt.close(fig)
