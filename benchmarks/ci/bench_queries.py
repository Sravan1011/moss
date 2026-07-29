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


def build_fingerprint() -> str:
    """Fingerprint of the code path that builds the index.

    Covers the installed SDK/bindings versions and, when running from the
    repository, a content hash of the Python SDK source tree. Any change to
    the indexing/build path yields a new fingerprint — and therefore a new
    index name via ``index_name_for`` — so the benchmark rebuilds the index
    and exercises ``create_index``/document serialization instead of loading
    an index built by older code (which could pass on stale embeddings).
    """
    from importlib.metadata import PackageNotFoundError, version

    h = hashlib.sha256()
    for pkg in ("moss", "inferedge-moss", "inferedge-moss-core"):
        try:
            h.update(f"{pkg}={version(pkg)}".encode())
        except PackageNotFoundError:
            pass
    sdk_src = Path(__file__).resolve().parents[2] / "sdks" / "python" / "sdk" / "src"
    if sdk_src.is_dir():
        for p in sorted(sdk_src.rglob("*.py")):
            h.update(str(p.relative_to(sdk_src)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()[:12]


def index_name_for(signature: str, fingerprint: str) -> str:
    """Benchmark index name derived from data signature + build fingerprint.

    The name is the index's manifest: it changes whenever the corpus slice,
    DOC_COUNT, model, or the SDK build path changes, so a stale remote index
    can never be silently reused against mismatched inputs or code.
    """
    return f"{INDEX_NAME_PREFIX}-{signature}-{fingerprint}"


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
