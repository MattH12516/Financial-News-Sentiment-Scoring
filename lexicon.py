"""Dump the merged sentiment lexicon (VADER + LM-via-GloVe + manual patches) to CSV."""

import csv
from news_scoring import _analyzer

with open("full_lexicon.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Word", "Score"])
    for word, score in sorted(_analyzer.lexicon.items()):
        writer.writerow([word, round(score, 3)])

print(f"Wrote {len(_analyzer.lexicon)} entries to full_lexicon.csv")