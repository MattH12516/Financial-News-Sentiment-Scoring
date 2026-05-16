import trafilatura

# Pick a recent Yahoo URL from your output
url = "https://finance.yahoo.com/markets/stocks/articles/tesla-toyota-expose-surprising-auto-043700448.html"

print(f"Trying: {url}")
downloaded = trafilatura.fetch_url(url)
print(f"Downloaded: {len(downloaded) if downloaded else 'None'} bytes")

if downloaded:
    text = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
    print(f"Extracted: {len(text) if text else 'None'} chars")
    if text:
        print("\nFirst 500 chars:")
        print(text[:500])