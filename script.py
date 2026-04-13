"""
Zonos Classification Scraper
----------------------------
Reads a CSV, calls the Zonos classificationsCalculate GraphQL API for each row,
and writes all results back to a new CSV with enriched classification columns.

Required pip packages:
    pip install requests tqdm

Usage:
    python zonos_classifier.py --input products.csv --output classified.csv

Expected CSV columns (case-sensitive):
    Handle, Title, SKU, COO (country of origin), category
"""

import argparse
import csv
import json
import sys
import time
import requests
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  –  edit these values before running
# ─────────────────────────────────────────────────────────────────────────────
CREDENTIAL_TOKEN   = "credential_live_cs_f09b552d-3791-4b13-9cbb-e25efcda78d0"
ORGANIZATION_ID    = "organization_0mzkcnx1z1sky"
ORGANIZATION_STATUS = "TRANSACTING"

# Paste your full Cookie header value here (copy from browser DevTools → Network)
COOKIE = "__client_uat=1773550430; __client_uat_IVzf_GCa=1773550430; zonos-user-token=credential_live_cs_f09b552d-3791-4b13-9cbb-e25efcda78d0; __refresh_IVzf_GCa=WoaQ5dUJxhew2Xn9nEmF; clerk_active_context=sess_3Ay2L1v9ujytrAlOJH3to7rLNbo:; _delighted_web={%223HcEQwMDb6ervPGe%22:{%22_delighted_fst%22:{%22t%22:%221773550440744%22}}}; __stripe_mid=c16fbe60-3be4-4aaa-9514-22d8492d8579d4541b; __stripe_sid=fbe53940-24cc-487f-af7c-3ce25f66df5d63f1a0; _ga=GA1.1.1160230274.1773550718; _gcl_au=1.1.925715247.1773550718; 5a77b402a4922b50_cfid=1db0cca04921888d3215978e5c1a096e781d5d7f962c5217acb726d45dd292a489671ed4c12b340922704b6d6c0ed35ca35e4bf4fc29a3c7d210ceb80851bf32; fs_uid=^#NQFME^#5c164451-fce9-45b8-a477-cbc39f7abc73:44b65f5e-9abb-4f0b-bfe6-9b55f2b7c0d1:1773550353188::4^#4afe4830^#/1792673150; _ga_NW4WV1RPSM=GS2.1.s1773550717^$o1^$g1^$t1773553661^$j57^$l0^$h0; fs_lua=1.1773554095193; __session=eyJhbGciOiJSUzI1NiIsImNhdCI6ImNsX0I3ZDRQRDExMUFBQSIsImtpZCI6Imluc18yZnNkdEhyYmF6emYzQW5WSHA0ZGlqNkNJNzkiLCJ0eXAiOiJKV1QifQ.eyJhenAiOiJodHRwczovL2Rhc2hib2FyZC56b25vcy5jb20iLCJlbWFpbCI6InJhZmlrc2hhaWlraDI1QGdtYWlsLmNvbSIsImV4cCI6MTc3MzU1NDQxMSwiZnZhIjpbNjUsLTFdLCJpYXQiOjE3NzM1NTQzNTEsImlzcyI6Imh0dHBzOi8vY2xlcmsuem9ub3MuY29tIiwianRpIjoiODY3ZTZlYzZhMDk4OGMzZWIyOTkiLCJuYmYiOjE3NzM1NTQzNDEsInBkYXRhIjp7Inpyb2xlcyI6WyJEYXNoYm9hcmQgTGl0ZSIsIkFkbWluIl19LCJzaWQiOiJzZXNzXzNBeTJMMXY5dWp5dHJBbE9KSDN0bzdyTE5ibyIsInN0cyI6ImFjdGl2ZSIsInN1YiI6InVzZXJfMzVJWjVJdzU1cTR6emVkYTJ0c3FId3N0bFRWIiwiem9pZCI6bnVsbCwienJvbGVzIjpbIkRhc2hib2FyZCBMaXRlIiwiQWRtaW4iXSwienVpZCI6InVzZXJfZGI0MTNmNTItODNjZS00NTgyLTlhYTEtOTBmZmRmYzY4YTdkIn0.OzP2bEewUxMtpWmwa-x64Rmehpf8grxsK2EznrUnqHgD5R-S-RHJeVb6ugbsr4ryCZ7T7FhHHt3QGDL3cWH_D5r2wdwvUSwTfF7FYL3HHHkoV1r-V9T5wfHTAu0nP6z3g-PfdyoAK78KwUcRS5pvNnZZxj6SesPvNok9yHztOthZ3qsdTVkeD9wWQT3ky4bpxi3JTFW9xPkD9YEpym6Gyperh5WW4jkA7JRSEeDLmMC0s6bL5pHH0GdN9t4wvK5QM9JL_ZnNFJ8H52oaIw-oH4uPqp0TOuBa6FTItpkhJkBIuIGspFwciOwLMN-9EyIUYTVfj4pUqaDC2cfF10vcuQ; __session_IVzf_GCa=eyJhbGciOiJSUzI1NiIsImNhdCI6ImNsX0I3ZDRQRDExMUFBQSIsImtpZCI6Imluc18yZnNkdEhyYmF6emYzQW5WSHA0ZGlqNkNJNzkiLCJ0eXAiOiJKV1QifQ.eyJhenAiOiJodHRwczovL2Rhc2hib2FyZC56b25vcy5jb20iLCJlbWFpbCI6InJhZmlrc2hhaWlraDI1QGdtYWlsLmNvbSIsImV4cCI6MTc3MzU1NDQxMSwiZnZhIjpbNjUsLTFdLCJpYXQiOjE3NzM1NTQzNTEsImlzcyI6Imh0dHBzOi8vY2xlcmsuem9ub3MuY29tIiwianRpIjoiODY3ZTZlYzZhMDk4OGMzZWIyOTkiLCJuYmYiOjE3NzM1NTQzNDEsInBkYXRhIjp7Inpyb2xlcyI6WyJEYXNoYm9hcmQgTGl0ZSIsIkFkbWluIl19LCJzaWQiOiJzZXNzXzNBeTJMMXY5dWp5dHJBbE9KSDN0bzdyTE5ibyIsInN0cyI6ImFjdGl2ZSIsInN1YiI6InVzZXJfMzVJWjVJdzU1cTR6emVkYTJ0c3FId3N0bFRWIiwiem9pZCI6bnVsbCwienJvbGVzIjpbIkRhc2hib2FyZCBMaXRlIiwiQWRtaW4iXSwienVpZCI6InVzZXJfZGI0MTNmNTItODNjZS00NTgyLTlhYTEtOTBmZmRmYzY4YTdkIn0.OzP2bEewUxMtpWmwa-x64Rmehpf8grxsK2EznrUnqHgD5R-S-RHJeVb6ugbsr4ryCZ7T7FhHHt3QGDL3cWH_D5r2wdwvUSwTfF7FYL3HHHkoV1r-V9T5wfHTAu0nP6z3g-PfdyoAK78KwUcRS5pvNnZZxj6SesPvNok9yHztOthZ3qsdTVkeD9wWQT3ky4bpxi3JTFW9xPkD9YEpym6Gyperh5WW4jkA7JRSEeDLmMC0s6bL5pHH0GdN9t4wvK5QM9JL_ZnNFJ8H52oaIw-oH4uPqp0TOuBa6FTItpkhJkBIuIGspFwciOwLMN-9EyIUYTVfj4pUqaDC2cfF10vcuQ"

# Seconds to wait between requests (be polite to the API)
REQUEST_DELAY = 0.3

# Number of retries on transient errors (5xx / network issues)
MAX_RETRIES = 3
RETRY_BACKOFF = 2   # seconds – doubles on each retry

API_URL = "https://dashboard.zonos.com/api/graphql/internal/classificationsCalculate"
# ─────────────────────────────────────────────────────────────────────────────

GRAPHQL_QUERY = """
mutation classificationsCalculate($level: ClassificationLevel!, $inputs: [ClassificationCalculateInput!]!) {
  classificationsCalculate(input: $inputs, level: $level) {
    ...ClassificationResultFieldsWithAlternates
  }
}

fragment ClassificationResultFieldsWithAlternates on Classification {
  ...ClassificationResultFieldsBase
  configuration {
    hsCodeProvidedTreatment
  }
  alternates {
    ...ClassificationAlternateFields
  }
}

fragment ClassificationResultFieldsBase on Classification {
  id
  confidenceScore
  customsDescription
  hsCode {
    code
    description {
      friendly
      full
      fullTruncated
    }
    fragments {
      code
      description
      type
    }
  }
  hsCodeProvidedValidation {
    code
    result
    type
  }
}

fragment ClassificationAlternateFields on ClassificationAlternate {
  subheadingAlternate {
    code
    description {
      friendly
      full
      fullTruncated
    }
    fragments {
      code
      description
      type
    }
  }
}
"""

HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "credentialtoken": CREDENTIAL_TOKEN,
    "origin": "https://dashboard.zonos.com",
    "referer": "https://dashboard.zonos.com/products/classify/create",
    "x-organization-id": ORGANIZATION_ID,
    "x-organization-status": ORGANIZATION_STATUS,
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Cookie": COOKIE,
}

# New columns that will be appended to every output row
NEW_COLUMNS = [
    "status",
    "classificationId",
    "confidenceScore",
    "customsDescription",
    "hsCode_code",
    "hsCode_description_friendly",
    "CHAPTER_code",
    "CHAPTER_description",
    "HEADING_code",
    "HEADING_description",
    "SUBHEADING_code",
    "SUBHEADING_description",
    "TARIFF_code",
    "TARIFF_description",
]


def build_payload(row: dict) -> dict:
    """Build the GraphQL request payload from a CSV row."""
    sku        = row.get("SKU", "").strip()
    handle     = row.get("Handle", "").strip()
    title      = row.get("Title", "").strip()
    coo        = row.get("COO", "").strip()
    category  = row.get("category", "").strip()
    image  = row.get("image", "").strip()
    vendor  = row.get("vendor", "").strip()
    description  = row.get("description", "").strip()
    type  = row.get("type", "").strip()
    barcode  = row.get("Barcode", "").strip()
    weight  = row.get("weight", "").strip()
    weight_unit  = row.get("weight unit", "").strip()



    return {
        "query": GRAPHQL_QUERY,
        "variables": {
            "level": "ULTRA",
            "inputs": [
                {
                    "categories": [category] if category else [],
                    "countryOfOrigin": coo,
                    "configuration": {
                        "hsCodeProvided": "",
                        "hsCodeProvidedTreatment": "IGNORE",
                        "shipFromCountry": "CA",
                        "sku": sku,
                        "shipToCountries": ["US"],
                    },
                    "description": f"Vendor= {vendor}, Short Description: {description}, Type: {type}, Barcode: {barcode}, Weight: {weight} {weight_unit}",
                    "imageUrl": image,
                    "material": "",
                    "name": title,
                    "productUrl": f"https://www.harbourchandler.ca/products/{handle}",
                }
            ],
        },
    }


def extract_fragments(fragments: list) -> dict:
    """Return a flat dict of {TYPE_code, TYPE_description} for each fragment type."""
    result = {}
    for frag in fragments or []:
        t = frag.get("type", "UNKNOWN").upper()
        result[f"{t}_code"]        = frag.get("code", "")
        result[f"{t}_description"] = frag.get("description", "")
    return result


def call_api(payload: dict) -> tuple[int, dict | None]:
    """
    POST to the Zonos API with retries.
    Returns (http_status_code, parsed_json_or_None).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
            if resp.status_code == 200:
                return 200, resp.json()
            elif resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                tqdm.write(f"    ⚠  HTTP {resp.status_code} – retrying in {wait}s "
                           f"(attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                return resp.status_code, None
        except requests.RequestException as exc:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                tqdm.write(f"    ⚠  Network error ({exc}) – retrying in {wait}s")
                time.sleep(wait)
            else:
                return -1, None
    return -1, None


def parse_classification(data: dict) -> dict:
    """Pull the fields we care about out of the API response JSON."""
    result = {col: "" for col in NEW_COLUMNS}

    classifications = (
        data.get("data", {})
            .get("classificationsCalculate", [])
    )
    if not classifications:
        return result

    c = classifications[0]   # we send one input → one result

    result["classificationId"]           = c.get("id", "")
    result["confidenceScore"]            = c.get("confidenceScore", "")
    result["customsDescription"]         = c.get("customsDescription", "")

    hs = c.get("hsCode") or {}
    result["hsCode_code"]                = hs.get("code", "")
    result["hsCode_description_friendly"] = (
        (hs.get("description") or {}).get("friendly", "")
    )

    frags = extract_fragments(hs.get("fragments", []))
    for key, val in frags.items():
        if key in result:
            result[key] = val

    return result


def process_csv(input_path: str, output_path: str) -> None:
    # ── Read input CSV ──────────────────────────────────────────────────────
    with open(input_path, newline="", encoding="utf-8-sig") as f:
        reader    = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows       = list(reader)

    if not rows:
        print("⚠  Input CSV is empty.")
        return

    # ── Prepare output field list (original cols + new cols) ────────────────
    out_fields = fieldnames.copy()
    for col in NEW_COLUMNS:
        if col not in out_fields:
            out_fields.append(col)

    success_count = 0
    error_count   = 0

    # ── Open output CSV ─────────────────────────────────────────────────────
    with open(output_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()

        # ── Main loop with progress bar ─────────────────────────────────────
        with tqdm(
            total=len(rows),
            desc="Classifying",
            unit="row",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            colour="cyan",
        ) as pbar:

            for row in rows:
                sku  = row.get("SKU", "").strip() or "(no SKU)"
                pbar.set_postfix_str(f"SKU={sku}")

                payload            = build_payload(row)
                http_status, data  = call_api(payload)

                enriched = {col: "" for col in NEW_COLUMNS}
                enriched["status"] = str(http_status)

                if http_status == 200 and data:
                    classification_data = parse_classification(data)
                    enriched.update(classification_data)
                    enriched["status"] = "200"
                    success_count += 1
                else:
                    error_msg = f"ERROR_{http_status}"
                    enriched["status"] = error_msg
                    error_count += 1
                    tqdm.write(f"    ✗  Row SKU={sku}  →  {error_msg}")

                row.update(enriched)
                writer.writerow(row)
                out_f.flush()     # write incrementally so partial results are saved

                pbar.update(1)
                time.sleep(REQUEST_DELAY)

    # ── Summary ─────────────────────────────────────────────────────────────
    total = len(rows)
    print(f"\n{'─'*55}")
    print(f"  Done!  Total: {total}  ✓ Success: {success_count}  ✗ Errors: {error_count}")
    print(f"  Output saved to: {output_path}")
    print(f"{'─'*55}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry-point
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Classify Shopify products via the Zonos GraphQL API."
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to the input CSV file  (e.g. products.csv)"
    )
    parser.add_argument(
        "--output", "-o",
        default="classified_output.csv",
        help="Path for the output CSV file  (default: classified_output.csv)"
    )
    args = parser.parse_args()

    if COOKIE == "PASTE_YOUR_COOKIE_HERE":
        print("⚠  ERROR: You must paste your browser Cookie value into the COOKIE variable at the top of this script.")
        sys.exit(1)

    process_csv(args.input, args.output)


if __name__ == "__main__":
    main()