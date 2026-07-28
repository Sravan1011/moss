"""Shared configuration for the CI benchmark suite.

Both the benchmark tests (``test_bench_ci_moss.py``) and the ground-truth
generator (``generate_ground_truth.py``) import from this module so the
query set, index naming, and corpus signature can never drift between
generation and evaluation.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

INDEX_NAME_PREFIX = "benchmark-ci"
MODEL_ID = "moss-minilm"
DOC_COUNT = 1_000


def load_corpus_slice(corpus_path: Path) -> list[dict[str, Any]]:
    """Load the first ``DOC_COUNT`` documents of the shared corpus file."""
    with open(corpus_path) as f:
        all_docs = json.load(f)
    return all_docs[:DOC_COUNT]


def corpus_signature(docs: list[dict[str, Any]]) -> str:
    """Short content hash of everything the benchmark index is built from.

    Covers the model id, DOC_COUNT, and the exact corpus slice that gets
    indexed. Any change to those inputs yields a different signature — and
    therefore a different index name via ``index_name_for`` — so a stale
    remote index can never be silently reused against mismatched data.
    """
    h = hashlib.sha256()
    h.update(MODEL_ID.encode())
    h.update(str(DOC_COUNT).encode())
    for d in docs:
        h.update(json.dumps(d, sort_keys=True).encode())
    return h.hexdigest()[:12]


def index_name_for(signature: str) -> str:
    """Benchmark index name derived from the corpus/model signature."""
    return f"{INDEX_NAME_PREFIX}-{signature}"


def query_set_hash() -> str:
    """Short hash of the benchmark query set.

    Stored in ``ground_truth.json`` and in every results file's config so
    the recall test and the regression guard can detect a query set that
    drifted from the one used at generation/baseline time.
    """
    h = hashlib.sha256()
    for q in QUERIES:
        h.update(q.encode())
        h.update(b"\x00")
    return h.hexdigest()[:12]

QUERIES = [
    "neural network training data",
    "anomaly detection patterns",
    "computer vision image processing",
    "natural language processing",
    "reinforcement learning rewards",
    "transfer learning pretrained models",
    "distributed computing systems",
    "cryptographic data encryption",
    "database indexing performance",
    "knowledge graph entities",
    "generative adversarial networks",
    "attention mechanism transformers",
    "dimensionality reduction compression",
    "federated learning privacy",
    "stream processing pipelines",
]
