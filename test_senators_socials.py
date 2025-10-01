import csv
import requests

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

def clean_handle(h: str) -> str:
    if not h:
        return ""
    h = h.strip()
    if h in {"???", "N/A", "-", "none", "null"}:
        return ""
    return h.lstrip("@").strip()

def build_url(kind: str, value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if v.startswith("http://") or v.startswith("https://"):
        return v

    if kind == "twitter":
        return f"https://x.com/{clean_handle(v)}"
    if kind == "facebook":
        return f"https://www.facebook.com/{clean_handle(v)}"
    if kind == "instagram":
        return f"https://www.instagram.com/{clean_handle(v)}"
    if kind == "youtube":
        # Accept channel IDs (UC...), @handles, or plain names
        if v.startswith("@"):
            return f"https://www.youtube.com/{v}"
        if v.startswith("UC") and len(v) >= 20:
            return f"https://www.youtube.com/channel/{v}"
        return f"https://www.youtube.com/@{clean_handle(v)}"
    return v

def validate_url(url: str, site: str | None = None, timeout: int = 10) -> bool | str:
    """Return True/False for known value, '' if no URL."""
    if not url:
        return ""
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        # Many social sites block HEAD; fall back to GET
        if r.status_code in (405, 403, 429) or r.status_code >= 400:
            r = requests.get(url, allow_redirects=True, timeout=timeout, headers=headers)

        ok = 200 <= r.status_code < 400 or bool(getattr(r, "is_redirect", False))
        if not ok:
            return False

        if site == "twitter":
            # Reduce false positives where X returns 200 + “doesn’t exist/suspended”
            try:
                txt = r.text.lower()
            except Exception:
                txt = ""
            if (
                "account doesn’t exist" in txt
                or "account doesn't exist" in txt
                or "account suspended" in txt
            ):
                return False
        return True
    except requests.RequestException:
        return False

def main(in_path="senators_socials.csv", out_path="senators_socials_validated.csv"):
    with open(in_path, newline="", encoding="utf-8") as infile:
        # Capture overflow columns in '_rest' instead of None
        reader = csv.DictReader(infile, restkey="_rest", restval="")

        base_fields = reader.fieldnames or []
        extra_fields = [
            "twitter_url", "facebook_url", "instagram_url", "youtube_url",
            "twitter_url_valid", "facebook_url_valid", "instagram_url_valid", "youtube_url_valid",
        ]
        fieldnames = base_fields + extra_fields

        with open(out_path, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for i, row in enumerate(reader, start=2):  # header is line 1
                # Nuke stray columns/comments that create the None key problem
                row.pop(None, None)
                row.pop("_rest", None)

                # Normalize + build URLs
                tw = build_url("twitter", row.get("twitter", ""))
                fb = build_url("facebook", row.get("facebook", ""))
                ig = build_url("instagram", row.get("instagram", ""))
                yt = build_url("youtube", row.get("youtube", ""))

                row["twitter_url"] = tw
                row["facebook_url"] = fb
                row["instagram_url"] = ig
                row["youtube_url"] = yt

                # Validate (broadly) — prefer fewer false negatives
                row["twitter_url_valid"] = validate_url(tw, site="twitter")
                row["facebook_url_valid"] = validate_url(fb)
                row["instagram_url_valid"] = validate_url(ig)
                row["youtube_url_valid"] = validate_url(yt)

                # Only write known fields
                out_row = {k: row.get(k, "") for k in fieldnames}
                writer.writerow(out_row)

    print(f"Validation complete. Wrote: {out_path}")

if __name__ == "__main__":
    main()
