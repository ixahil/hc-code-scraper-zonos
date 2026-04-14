import csv
import html
import os
import re
import sys
import json
from pathlib import Path

 
import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request
from tqdm import tqdm
from dotenv import load_dotenv
load_dotenv()  # loads .env into os.environ automatically

# ZONOS_CREDENTIAL_TOKEN = 
# ZONOS_ORGANIZATION_ID = 
# ZONOS_ORGANIZATION_STATUS = 
# ZONOS_COOKIE  = 

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
 
API_URL = "https://dashboard.zonos.com/api/graphql/internal/classificationsCalculate"
 
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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
 
def html_to_text(value: str) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    return " ".join(text.split())
 
 
def build_payload(row: dict) -> dict:
    sku          = row.get("SKU", "").strip()
    handle       = row.get("Handle", "").strip()
    title        = row.get("Title", "").strip()
    coo          = row.get("COO", "").strip()
    category     = row.get("Category", "").strip()
    image        = row.get("Image", "").strip()
    vendor       = row.get("Vendor", "").strip()
    description  = html_to_text(row.get("Description", ""))
    barcode      = row.get("Barcode", "").strip()
    weight       = row.get("Weight", "").strip()
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
                    "description": (
                        f"Vendor= {vendor}, Short Description: {description}, "
                        f"Barcode: {barcode}, Weight: {weight} {weight_unit}"
                    ),
                    "imageUrl": image,
                    "material": "",
                    "name": title,
                    "productUrl": f"https://www.harbourchandler.ca/products/{handle}",
                }
            ],
        },
    }
 
 
def extract_fragments(fragments: list) -> dict:
    result = {}
    for frag in fragments or []:
        t = frag.get("type", "UNKNOWN").upper()
        result[f"{t}_code"]        = frag.get("code", "")
        result[f"{t}_description"] = frag.get("description", "")
    return result
 
 
def parse_classification(data: dict) -> dict:
    result = {col: "" for col in NEW_COLUMNS}
 
    classifications = data.get("data", {}).get("classificationsCalculate", [])
    if not classifications:
        return result
 
    c = classifications[0]
 
    result["classificationId"]            = c.get("id", "")
    result["confidenceScore"]             = c.get("confidenceScore", "")
    result["customsDescription"]          = c.get("customsDescription", "")
 
    hs = c.get("hsCode") or {}
    code = hs.get("code", "")
    result["hsCode_code"] = f"=\"{code}\"" if code else ""  
    result["hsCode_description_friendly"] = (hs.get("description") or {}).get("friendly", "")
 
    frags = extract_fragments(hs.get("fragments", []))
    for key, val in frags.items():
        if key in result:
            result[key] = val
 
    return result
 

class ZonosClassifySpider(scrapy.Spider):
    name = "zonos-classify"
    allowed_domains = ["dashboard.zonos.com"]
    # start_urls = ["https://dashboard.zonos.com"]

    def __init__(self, input=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
 
        if not input:
            raise ValueError("Provide -a input=<path/to/file.csv>")
 
        input_path  = Path(input)
        output_path = input_path.parent / f"{input_path.stem}-classified.csv"
 
        self.input_path  = input_path
        self.output_path = output_path
 
        # ── Auth from env vars ───────────────────────────────────────────────
        self.credential_token    = os.environ["ZONOS_CREDENTIAL_TOKEN"]
        self.organization_id     = os.environ["ZONOS_ORGANIZATION_ID"]
        self.organization_status = os.environ["ZONOS_ORGANIZATION_STATUS"]
        self.cookie              = os.environ["ZONOS_COOKIE"]
 
        # ── Read CSV up front ────────────────────────────────────────────────
        with open(input_path, newline="", encoding="utf-8-sig") as f:
            reader         = csv.DictReader(f)
            self.fieldnames = list(reader.fieldnames or [])
            self.rows       = list(reader)
 
        # ── Prepare output field list ────────────────────────────────────────
        self.out_fields = self.fieldnames.copy()
        for col in NEW_COLUMNS:
            if col not in self.out_fields:
                self.out_fields.append(col)
 
        # ── Open output CSV ──────────────────────────────────────────────────
        self._out_f  = open(output_path, "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(
            self._out_f, fieldnames=self.out_fields, extrasaction="ignore"
        )
        self._writer.writeheader()
 
        # ── Counters ─────────────────────────────────────────────────────────
        self.success_count = 0
        self.error_count   = 0
        self.total         = len(self.rows)
 
        # ── Progress bar ─────────────────────────────────────────────────────
        self.pbar = tqdm(
            total=self.total,
            desc="Classifying",
            unit="row",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            colour="cyan",
        )
 
        print(f"  Input : {input_path}")
        print(f"  Output: {output_path}")
        print(f"  Rows  : {self.total}\n")
 
    # ── Scrapy lifecycle ─────────────────────────────────────────────────────
 
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
 
    def spider_closed(self, spider):
        self.pbar.close()
        self._out_f.flush()
        self._out_f.close()
        print(f"\n{'─'*55}")
        print(f"  Done!  Total: {self.total}  "
              f"✓ Success: {self.success_count}  ✗ Errors: {self.error_count}")
        print(f"  Output saved to: {self.output_path}")
        print(f"{'─'*55}")
 
    # ── Request generation ───────────────────────────────────────────────────
 
    def start_requests(self):
        for idx, row in enumerate(self.rows):
            coo = row.get("COO", "").strip()
            sku = row.get("SKU", "").strip() or "(no SKU)"

            if not coo:
                enriched = {col: "" for col in NEW_COLUMNS}
                enriched["status"] = "COO_NEEDED"

                row.update(enriched)
                self._writer.writerow(row)
                self._out_f.flush()

                self.error_count += 1
                self.pbar.set_postfix_str(f"SKU={sku} → COO_NEEDED")
                self.pbar.update(1)

                continue

            payload = build_payload(row)


            yield Request(
                url=API_URL,
                method="POST",
                headers={
                    "accept":               "*/*",
                    "accept-language":      "en-US,en;q=0.9",
                    "content-type":         "application/json",
                    "credentialtoken":      self.credential_token,
                    "origin":               "https://dashboard.zonos.com",
                    "referer":              "https://dashboard.zonos.com/products/classify/create",
                    "x-organization-id":    self.organization_id,
                    "x-organization-status": self.organization_status,
                    "user-agent":           (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/146.0.0.0 Safari/537.36"
                    ),
                    "Cookie": self.cookie,
                },
                body=json.dumps(payload).encode(),
                cb_kwargs={"row": row, "idx": idx},
                callback=self.parse_response,
                errback=self.handle_error,
                dont_filter=True,
            )
 
    # ── Response handling ────────────────────────────────────────────────────
 
    def parse_response(self, response, row, idx):
        sku = row.get("SKU", "").strip() or "(no SKU)"
 
        enriched = {col: "" for col in NEW_COLUMNS}
 
        if response.status == 200:
            try:
                data = response.json()
            except Exception:
                data = None
 
            if data:
                classification_data = parse_classification(data)
                enriched.update(classification_data)
                enriched["status"] = "200"
                self.success_count += 1
            else:
                enriched["status"] = "ERROR_PARSE"
                self.error_count   += 1
                tqdm.write(f"    ✗  SKU={sku}  →  ERROR_PARSE")
        else:
            enriched["status"] = f"ERROR_{response.status}"
            self.error_count   += 1
            tqdm.write(f"    ✗  SKU={sku}  →  ERROR_{response.status}")
 
        row.update(enriched)
        self._writer.writerow(row)
        self._out_f.flush()
 
        self.pbar.set_postfix_str(f"SKU={sku} → {enriched['status']}")
        self.pbar.update(1)
 
    def handle_error(self, failure):
        # Twisted errback — network-level failure (DNS, timeout, etc.)
        request = failure.request
        row     = request.cb_kwargs["row"]
        sku     = row.get("SKU", "").strip() or "(no SKU)"
 
        enriched = {col: "" for col in NEW_COLUMNS}
        enriched["status"] = "ERROR_NETWORK"
        self.error_count  += 1
 
        row.update(enriched)
        self._writer.writerow(row)
        self._out_f.flush()
 
        self.pbar.set_postfix_str(f"SKU={sku} → ERROR_NETWORK")
        tqdm.write(f"    ✗  SKU={sku}  →  ERROR_NETWORK  ({failure.getErrorMessage()})")
        self.pbar.update(1)