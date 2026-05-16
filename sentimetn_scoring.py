"""
Compute intensity scores for Loughran-McDonald words using GloVe embeddings.
Projects each LM word onto a sentiment axis defined by anchor words.
Caches results to lm_glove_scores.json — re-run this script to refresh.
"""

import ssl
import certifi
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

import csv
import json
import gensim.downloader as api
import numpy as np

LM_DICT_PATH = "Loughran-McDonald_MasterDictionary_1993-2025.csv"
OUTPUT_PATH = "lm_glove_scores.json"

POS_ANCHORS = [
    "excellent", "profitable", "outstanding", "successful",
    "beneficial", "achievement", "gains", "thriving",
]
NEG_ANCHORS = [
    "bankruptcy", "fraud", "failure", "disastrous",
    "collapse", "losses", "lawsuit", "terrible",
]


def load_lm_words():
    """Return a single set of all LM positive + negative words (lowercased)."""
    words = set()
    with open(LM_DICT_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            word = row["Word"].lower().strip()
            if not word:
                continue
            pos = int(row.get("Positive", 0) or 0) > 0
            neg = int(row.get("Negative", 0) or 0) > 0
            if pos or neg:
                words.add(word)
    return words


def main():
    print("Loading GloVe embeddings (first time downloads ~128MB)...")
    glove = api.load("glove-wiki-gigaword-100")
    print(f"GloVe loaded: {len(glove)} words in vocabulary.\n")

    # Build the sentiment axis
    pos_vecs = [glove[w] for w in POS_ANCHORS if w in glove]
    neg_vecs = [glove[w] for w in NEG_ANCHORS if w in glove]
    print(f"Anchor coverage: {len(pos_vecs)}/{len(POS_ANCHORS)} positive, "
          f"{len(neg_vecs)}/{len(NEG_ANCHORS)} negative")

    pos_centroid = np.mean(pos_vecs, axis=0)
    neg_centroid = np.mean(neg_vecs, axis=0)
    axis = pos_centroid - neg_centroid
    axis = axis / np.linalg.norm(axis)  # unit-length

    # Project every LM word onto the axis
    lm_words = load_lm_words()
    print(f"\nLM words to score: {len(lm_words)}")

    raw_scores = {}
    missing = 0
    for word in lm_words:
        if word in glove:
            raw_scores[word] = float(np.dot(glove[word], axis))
        else:
            missing += 1

    print(f"  Scored: {len(raw_scores)}  |  Missing from GloVe vocab: {missing}")

    # Scale to roughly VADER's -3.5 to +3.5 range
    max_abs = max(abs(v) for v in raw_scores.values())
    scale = 3.5 / max_abs if max_abs > 0 else 1.0
    scaled_scores = {w: round(s * scale, 3) for w, s in raw_scores.items()}

    # Save to JSON
    with open(OUTPUT_PATH, "w") as f:
        json.dump(scaled_scores, f, indent=2, sort_keys=True)

    # Print a sanity-check distribution
    sorted_scores = sorted(scaled_scores.items(), key=lambda x: x[1])
    print(f"\nWrote {len(scaled_scores)} scored words to {OUTPUT_PATH}")
    print(f"\nMost negative 10:")
    for w, s in sorted_scores[:10]:
        print(f"  {s:+.2f}  {w}")
    print(f"\nMost positive 10:")
    for w, s in sorted_scores[-10:]:
        print(f"  {s:+.2f}  {w}")


if __name__ == "__main__":
    main()