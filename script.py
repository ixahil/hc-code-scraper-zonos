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
import html
import json
import re
import sys
import time
import requests
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  –  edit these values before running
# # ─────────────────────────────────────────────────────────────────────────────
# CREDENTIAL_TOKEN   = 
# ORGANIZATION_ID    = 
# ORGANIZATION_STATUS = 

# # Paste your full Cookie header value here (copy from browser DevTools → Network)
# COOKIE = 

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
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Cookie": COOKIE,
}


# curl ^"https://dashboard.zonos.com/api/graphql/internal/classificationsCalculate^" ^
#   -H ^"accept: */*^" ^
#   -H ^"accept-language: en-US,en;q=0.9,hi;q=0.8,es;q=0.7,fr;q=0.6,gu;q=0.5^" ^
#   -H ^"baggage: sentry-environment=vercel-production,sentry-release=faebf1d32f0eda4a7a92eb5e4996d5382befcda8,sentry-public_key=d01fba9379314aed920d125fb4d4851f,sentry-trace_id=61edb738520546aeb75227ccaa8afa45,sentry-org_id=446983,sentry-transaction=^%^2Fproducts^%^2Fclassify^%^2Fcreate,sentry-sampled=true,sentry-sample_rand=0.2210668940117908,sentry-sample_rate=0.25^" ^
#   -H ^"content-type: application/json^" ^
#   -b ^"__stripe_mid=c16fbe60-3be4-4aaa-9514-22d8492d8579d4541b; _ga=GA1.1.1160230274.1773550718; _gcl_au=1.1.925715247.1773550718; 5a77b402a4922b50_cfid=1db0cca04921888d3215978e5c1a096e781d5d7f962c5217acb726d45dd292a489671ed4c12b340922704b6d6c0ed35ca35e4bf4fc29a3c7d210ceb80851bf32; __client_uat=1775833946; __client_uat_IVzf_GCa=1775833946; __refresh_IVzf_GCa=t1mhPM3PX78cs5xXCnre; _delighted_web=^{^%^223HcEQwMDb6ervPGe^%^22:^{^%^22_delighted_fst^%^22:^{^%^22t^%^22:^%^221773550440744^%^22^}^%^2C^%^22_delighted_lst^%^22:^{^%^22t^%^22:^%^221773844584000^%^22^%^2C^%^22m^%^22:^{^%^22token^%^22:^%^222coer4sSvFu2wZkhlluQgjUo^%^22^}^}^}^}; _ga_NW4WV1RPSM=GS2.1.s1776183372^$o7^$g0^$t1776183372^$j60^$l0^$h0; zonos-user-token=credential_live_cs_976b1334-d293-4bdf-953d-25bd3707c87b; clerk_active_context=sess_3CAgmHM8mRuNnim1OS9pXhDlpom:; fs_uid=^#NQFME^#5c164451-fce9-45b8-a477-cbc39f7abc73:d2d8a97a-7633-44c6-a103-e1b3181c4cb0:1776183373397::2^#4afe4830^#^#/1792673184; __stripe_sid=aae47c43-c82c-499a-b676-422d9c54f559ce9b9d; fs_lua=1.1776183477542; __session=eyJhbGciOiJSUzI1NiIsImNhdCI6ImNsX0I3ZDRQRDExMUFBQSIsImtpZCI6Imluc18yZnNkdEhyYmF6emYzQW5WSHA0ZGlqNkNJNzkiLCJ0eXAiOiJKV1QifQ.eyJhenAiOiJodHRwczovL2Rhc2hib2FyZC56b25vcy5jb20iLCJlbWFpbCI6InJhZmlrc2hhaWlraDI1QGdtYWlsLmNvbSIsImV4cCI6MTc3NjE4NDc1NCwiZnZhIjpbNTg0NSwtMV0sImlhdCI6MTc3NjE4NDY5NCwiaXNzIjoiaHR0cHM6Ly9jbGVyay56b25vcy5jb20iLCJqdGkiOiIwMGY4Y2I0YTY3YmQ0MWE5NDc1NCIsIm5iZiI6MTc3NjE4NDY4NCwicGRhdGEiOnsienJvbGVzIjpbIkRhc2hib2FyZCBMaXRlIiwiQWRtaW4iXX0sInNpZCI6InNlc3NfM0NBZ21ITThtUnVObmltMU9TOXBYaERscG9tIiwic3RzIjoiYWN0aXZlIiwic3ViIjoidXNlcl8zNUlaNUl3NTVxNHp6ZWRhMnRzcUh3c3RsVFYiLCJ6b2lkIjpudWxsLCJ6cm9sZXMiOlsiRGFzaGJvYXJkIExpdGUiLCJBZG1pbiJdLCJ6dWlkIjoidXNlcl9kYjQxM2Y1Mi04M2NlLTQ1ODItOWFhMS05MGZmZGZjNjhhN2QifQ.NgENrlDRgjkf4SDFdMLI-RuSmuGRLPrAraXKmYCrNoVW70lktNkp3xS_j4xlOOgsjneMQFgg9aiLTEp0AQWoh1y6YiSyHcr0nimbr-X2rUni6Q54BCktTt4UStrIcmEKN63pWXFUt8HWvLziuKJzGHNdLOanlunZtaTYd92-8f7lUY2sB7wjcHEYAGTnyTv07q-hhhQcRQdbAoVu29yWRIIpp9_9ng3xnzWa5lv5uZ7jIf6m8WmNcqUbKgjSU-FDrn9u3hNo0LAcaZ0bQJT5HjV1m4hoKquVtWdoakHLTIZ2XxLRDmWlydBfYTyGQbUSxHTRcOrB0f2r-O1YKJgxGA; __session_IVzf_GCa=eyJhbGciOiJSUzI1NiIsImNhdCI6ImNsX0I3ZDRQRDExMUFBQSIsImtpZCI6Imluc18yZnNkdEhyYmF6emYzQW5WSHA0ZGlqNkNJNzkiLCJ0eXAiOiJKV1QifQ.eyJhenAiOiJodHRwczovL2Rhc2hib2FyZC56b25vcy5jb20iLCJlbWFpbCI6InJhZmlrc2hhaWlraDI1QGdtYWlsLmNvbSIsImV4cCI6MTc3NjE4NDc1NCwiZnZhIjpbNTg0NSwtMV0sImlhdCI6MTc3NjE4NDY5NCwiaXNzIjoiaHR0cHM6Ly9jbGVyay56b25vcy5jb20iLCJqdGkiOiIwMGY4Y2I0YTY3YmQ0MWE5NDc1NCIsIm5iZiI6MTc3NjE4NDY4NCwicGRhdGEiOnsienJvbGVzIjpbIkRhc2hib2FyZCBMaXRlIiwiQWRtaW4iXX0sInNpZCI6InNlc3NfM0NBZ21ITThtUnVObmltMU9TOXBYaERscG9tIiwic3RzIjoiYWN0aXZlIiwic3ViIjoidXNlcl8zNUlaNUl3NTVxNHp6ZWRhMnRzcUh3c3RsVFYiLCJ6b2lkIjpudWxsLCJ6cm9sZXMiOlsiRGFzaGJvYXJkIExpdGUiLCJBZG1pbiJdLCJ6dWlkIjoidXNlcl9kYjQxM2Y1Mi04M2NlLTQ1ODItOWFhMS05MGZmZGZjNjhhN2QifQ.NgENrlDRgjkf4SDFdMLI-RuSmuGRLPrAraXKmYCrNoVW70lktNkp3xS_j4xlOOgsjneMQFgg9aiLTEp0AQWoh1y6YiSyHcr0nimbr-X2rUni6Q54BCktTt4UStrIcmEKN63pWXFUt8HWvLziuKJzGHNdLOanlunZtaTYd92-8f7lUY2sB7wjcHEYAGTnyTv07q-hhhQcRQdbAoVu29yWRIIpp9_9ng3xnzWa5lv5uZ7jIf6m8WmNcqUbKgjSU-FDrn9u3hNo0LAcaZ0bQJT5HjV1m4hoKquVtWdoakHLTIZ2XxLRDmWlydBfYTyGQbUSxHTRcOrB0f2r-O1YKJgxGA^" ^
#   -H ^"credentialtoken: credential_live_cs_976b1334-d293-4bdf-953d-25bd3707c87b^" ^
#   -H ^"origin: https://dashboard.zonos.com^" ^
#   -H ^"priority: u=1, i^" ^
#   -H ^"referer: https://dashboard.zonos.com/products/classify/create^" ^
#   -H ^"sec-ch-ua: ^\^"Chromium^\^";v=^\^"146^\^", ^\^"Not-A.Brand^\^";v=^\^"24^\^", ^\^"Google Chrome^\^";v=^\^"146^\^"^" ^
#   -H ^"sec-ch-ua-mobile: ?0^" ^
#   -H ^"sec-ch-ua-platform: ^\^"Windows^\^"^" ^
#   -H ^"sec-fetch-dest: empty^" ^
#   -H ^"sec-fetch-mode: cors^" ^
#   -H ^"sec-fetch-site: same-origin^" ^
#   -H ^"sentry-trace: 61edb738520546aeb75227ccaa8afa45-88f50ad28df03133-1^" ^
#   -H ^"user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36^" ^
#   -H ^"x-organization-id: organization_0mzkcnx1z1sky^" ^
#   -H ^"x-organization-status: TRANSACTING^" ^
#   --data-raw ^"^{^\^"query^\^":^\^"mutation classificationsCalculate(^$level: ClassificationLevel^!, ^$inputs: ^[ClassificationCalculateInput^!^]^!) ^{^\^\n  classificationsCalculate(input: ^$inputs, level: ^$level) ^{^\^\n    ...ClassificationResultFieldsWithAlternates^\^\n  ^}^\^\n^}^\^\n^\^\nfragment ClassificationResultFieldsWithAlternates on Classification ^{^\^\n  ...ClassificationResultFieldsBase^\^\n  configuration ^{^\^\n    hsCodeProvidedTreatment^\^\n  ^}^\^\n  alternates ^{^\^\n    ...ClassificationAlternateFields^\^\n  ^}^\^\n^}^\^\n^\^\nfragment ClassificationResultFieldsBase on Classification ^{^\^\n  id^\^\n  confidenceScore^\^\n  customsDescription^\^\n  hsCode ^{^\^\n    code^\^\n    description ^{^\^\n      friendly^\^\n      full^\^\n      fullTruncated^\^\n    ^}^\^\n    fragments ^{^\^\n      code^\^\n      description^\^\n      type^\^\n    ^}^\^\n  ^}^\^\n  hsCodeProvidedValidation ^{^\^\n    code^\^\n    result^\^\n    type^\^\n  ^}^\^\n^}^\^\n^\^\nfragment ClassificationAlternateFields on ClassificationAlternate ^{^\^\n  subheadingAlternate ^{^\^\n    code^\^\n    description ^{^\^\n      friendly^\^\n      full^\^\n      fullTruncated^\^\n    ^}^\^\n    fragments ^{^\^\n      code^\^\n      description^\^\n      type^\^\n    ^}^\^\n  ^}^\^\n^}^\^",^\^"variables^\^":^{^\^"inputs^\^":^{^\^"categories^\^":^[^],^\^"configuration^\^":^{^\^"hsCodeProvided^\^":^\^"^\^",^\^"hsCodeProvidedTreatment^\^":^\^"IGNORE^\^",^\^"shipToCountries^\^":^[^\^"US^\^"^]^},^\^"description^\^":^\^"^\^",^\^"imageUrl^\^":^\^"^\^",^\^"material^\^":^\^"^\^",^\^"name^\^":^\^"test^\^",^\^"productUrl^\^":null^},^\^"level^\^":^\^"BASE^\^"^},^\^"operationName^\^":^\^"classificationsCalculate^\^"^}^"



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


def html_to_text(value: str) -> str:
    """Convert HTML content into plain, normalized text."""
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    return " ".join(text.split())


def build_payload(row: dict) -> dict:
    """Build the GraphQL request payload from a CSV row."""
    sku        = row.get("SKU", "").strip()
    handle     = row.get("Handle", "").strip()
    title      = row.get("Title", "").strip()
    coo        = row.get("COO", "").strip()
    category  = row.get("Category", "").strip()
    image  = row.get("Image", "").strip()
    vendor  = row.get("Vendor", "").strip()
    description  = html_to_text(row.get("Description", ""))
    type  = row.get("Type", "").strip()
    barcode  = row.get("Barcode", "").strip()
    weight  = row.get("Weight", "").strip()
    weight_unit  = row.get("Weight Unit", "").strip()



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
            # print(resp.json())
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