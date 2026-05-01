from __future__ import annotations

from typing import Sequence

import numpy as np


def cosine_similarity(vec1: Sequence[float], vec2: Sequence[float]) -> float:
    """
    Cosine similarity mapped to [0, 1].

    - Returns 0.0 for empty vectors, mismatched dimensions, or zero-norm vectors.
    - Otherwise returns (cosine + 1) / 2 to map [-1, 1] -> [0, 1].
    """
    if not vec1 or not vec2:
        return 0.0

    if len(vec1) != len(vec2):
        return 0.0

    a = np.asarray(vec1, dtype=np.float32)
    b = np.asarray(vec2, dtype=np.float32)

    na = float(np.linalg.norm(a))
    nb = float(np.linalg.norm(b))
    if na == 0.0 or nb == 0.0:
        return 0.0

    cos = float(np.dot(a, b) / (na * nb))
    score = (cos + 1.0) / 2.0
    return float(np.clip(score, 0.0, 1.0))

