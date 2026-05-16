from news_scoring import _analyzer

words = ["soars", "plunge", "achieve", "abandon", "target", "block", 
         "beat", "miss", "rally", "tumble", "bankrupt", "growth"]

for word in words:
    score = _analyzer.lexicon.get(word)
    if score is None:
        print(f"  '{word}': NOT IN LEXICON")
    else:
        print(f"  '{word}': {score}")