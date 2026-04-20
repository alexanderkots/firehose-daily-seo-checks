#!/usr/bin/env python3
"""
Firehose Daily SEO Digest — taxesforexpats.com
"""

import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
from collections import defaultdict
from dataclasses import dataclass, field
from html import escape as html_escape


# ── Configuration ──────────────────────────────────────────────

TAP_TOKEN = os.environ.get("FIREHOSE_TAP_TOKEN", "")
BASE_URL = "https://api.firehose.com"
SINCE = os.environ.get("FIREHOSE_SINCE", "24h")
LIMIT = int(os.environ.get("FIREHOSE_LIMIT", "10000"))
TIMEOUT = int(os.environ.get("FIREHOSE_TIMEOUT", "120"))

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
RESEND_TO = os.environ.get("RESEND_TO", "")
RESEND_FROM = os.environ.get("RESEND_FROM", "digest@notprovided.eu")


# ── Data Model ─────────────────────────────────────────────────

@dataclass
class MatchedPage:
    url: str
    title: str
    matched_at: str
    rule_tag: str
    query_id: str
    page_type: str
    page_category: str
    language: str
    added_text: list = field(default_factory=list)
    removed_text: list = field(default_factory=list)
    added_anchors: list = field(default_factory=list)
    removed_anchors: list = field(default_factory=list)

    @property
    def change_type(self) -> str:
        has_added = bool(self.added_text)
        has_removed = bool(self.removed_text)
        if has_added and has_removed:
            return "UPDATED"
        elif has_removed:
            return "REMOVED"
        elif has_added:
            return "NEW"
        return "MATCH"

    @property
    def domain(self) -> str:
        try:
            from urllib.parse import urlparse
            return urlparse(self.url).netloc
        except Exception:
            return self.url

    @classmethod
    def from_event(cls, data: dict, rules: dict) -> "MatchedPage":
        doc = data.get("document", {})
        diff = doc.get("diff", {})
        chunks = diff.get("chunks", [])

        query_ids = data.get("query_ids", [])
        qid = query_ids[0] if query_ids else ""
        rule = rules.get(qid, {})

        return cls(
            url=doc.get("url", ""),
            title=doc.get("title", ""),
            matched_at=data.get("matched_at", ""),
            rule_tag=rule.get("tag", "unknown"),
            query_id=qid,
            page_type=", ".join(doc.get("page_types", [])),
            page_category=", ".join(doc.get("page_categories", [])),
            language=doc.get("language", ""),
            added_text=[c["text"] for c in chunks if c["typ"] == "ins"],
            removed_text=[c["text"] for c in chunks if c["typ"] == "del"],
            added_anchors=[c["text"] for c in chunks if c.get("typ") == "ins" and c.get("anchor")],
            removed_anchors=[c["text"] for c in chunks if c.get("typ") == "del" and c.get("anchor")],
        )


# ── API Functions ──────────────────────────────────────────────

def api_request(path: str, method: str = "GET", body: dict = None) -> dict:
    url = f"{BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {TAP_TOKEN}",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, headers=headers, data=data, method=method)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def fetch_rules() -> dict:
    data = api_request("/v1/rules")
    return {r["id"]: r for r in data.get("data", [])}


def consume_stream(rules: dict) -> list:
    url = f"{BASE_URL}/v1/stream?since={SINCE}&limit={LIMIT}&timeout={TIMEOUT}"
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {TAP_TOKEN}"}
    )

    pages = []
    skipped = 0
    event_type = None
    last_event_id = None

    with urllib.request.urlopen(req) as resp:
        for raw_line in resp:
            line = raw_line.decode("utf-8").strip()

            if not line:
                event_type = None
                continue

            if line.startswith("id:"):
                last_event_id = line[3:].strip()
            elif line.startswith("event:"):
                event_type = line[6:].strip()
            elif line.startswith("data:"):
                if event_type == "update":
                    try:
                        payload = json.loads(line[5:].strip())
                        query_ids = payload.get("query_ids", [])
                        qid = query_ids[0] if query_ids else ""
                        # Пропускаем события от удалённых/неизвестных rule ID
                        if qid not in rules:
                            skipped += 1
                            continue
                        pages.append(MatchedPage.from_event(payload, rules))
                    except json.JSONDecodeError as e:
                        print(f"  [WARN] Failed to parse event: {e}", file=sys.stderr)
                elif event_type == "error":
                    try:
                        err = json.loads(line[5:].strip())
                        print(f"  [ERROR] Stream error: {err.get('message', err)}", file=sys.stderr)
                    except json.JSONDecodeError:
                        print(f"  [ERROR] {line[5:].strip()}", file=sys.stderr)
                elif event_type == "end":
                    break

    print(f"  Last event ID: {last_event_id}")
    if skipped:
        print(f"  Skipped {skipped} events from unknown/deleted rules")
    return pages


# ── Digest Builder ─────────────────────────────────────────────

def build_digest(pages: list) -> str:
    by_tag = defaultdict(list)
    for p in pages:
        by_tag[p.rule_tag].append(p)

    now = datetime.now(timezone.utc)
    lines = []
    lines.append("=" * 50)
    lines.append(f"  FIREHOSE DAILY SEO DIGEST — taxesforexpats.com")
    lines.append(f"  {now:%A, %B %d, %Y} -- {now:%H:%M} UTC")
    lines.append(f"  Window: last {SINCE} | Events: {len(pages)}")
    lines.append("=" * 50)
    lines.append("")

    lines.append("SUMMARY")
    lines.append("-" * 40)
    for tag in sorted(by_tag.keys()):
        items = by_tag[tag]
        new = sum(1 for p in items if p.change_type == "NEW")
        updated = sum(1 for p in items if p.change_type == "UPDATED")
        removed = sum(1 for p in items if p.change_type == "REMOVED")
        lines.append(f"  [{tag}] {len(items)} total -- {new} new, {updated} updated, {removed} removed")
    lines.append("")

    for tag in sorted(by_tag.keys()):
        items = by_tag[tag]
        lines.append("=" * 50)
        lines.append(f"  [{tag.upper()}] -- {len(items)} events")
        lines.append("=" * 50)
        lines.append("")

        priority = {"NEW": 0, "UPDATED": 1, "REMOVED": 2, "MATCH": 3}
        items.sort(key=lambda p: (priority.get(p.change_type, 9), p.matched_at))

        for p in items[:25]:
            flag = f"[{p.change_type}]"
            lines.append(f"  {flag} {p.title or '(no title)'}")
            lines.append(f"         {p.url}")
            lines.append(f"         Domain: {p.domain}  |  Type: {p.page_type}  |  Lang: {p.language}")

            if p.added_text:
                preview = p.added_text[0][:150].replace("\n", " ")
                lines.append(f"         + Added: {preview}...")

            if p.removed_text:
                preview = p.removed_text[0][:150].replace("\n", " ")
                lines.append(f"         - Removed: {preview}...")

            lines.append("")

        if len(items) > 25:
            lines.append(f"  ... and {len(items) - 25} more events in [{tag}]")
            lines.append("")

    lines.append("=" * 50)
    lines.append(f"  End of digest. Generated {now:%H:%M:%S} UTC")
    lines.append("=" * 50)

    return "\n".join(lines)


def build_html_digest(pages: list) -> str:
    by_tag = defaultdict(list)
    for p in pages:
        by_tag[p.rule_tag].append(p)

    now = datetime.now(timezone.utc)

    html = []
    html.append(f"""
    <html><body style="font-family: -apple-system, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; color: #374151;">
    <h1 style="color: #111827; border-bottom: 3px solid #FF6B35; padding-bottom: 8px;">
        Firehose Daily Digest — taxesforexpats.com
    </h1>
    <p style="color: #6B7280;">{now:%A, %B %d, %Y} — {len(pages)} events in the last {SINCE}</p>
    """)

    tag_colors = {
        "comp-content": "#3B82F6",
        "brand":        "#FF6B35",
        "own-site":     "#6B7280",
    }

    for tag in sorted(by_tag.keys()):
        items = by_tag[tag]
        color = tag_colors.get(tag, "#6B7280")
        new_count = sum(1 for p in items if p.change_type == "NEW")
        removed_count = sum(1 for p in items if p.change_type == "REMOVED")

        html.append(f"""
        <h2 style="color: {color}; margin-top: 28px;">
            [{tag}] <span style="font-weight: normal; font-size: 14px; color: #6B7280;">
            {len(items)} events — {new_count} new, {removed_count} removed</span>
        </h2>
        """)

        priority = {"NEW": 0, "UPDATED": 1, "REMOVED": 2, "MATCH": 3}
        items.sort(key=lambda p: (priority.get(p.change_type, 9), p.matched_at))

        for p in items[:15]:
            badge_colors = {
                "NEW": "#22C55E", "UPDATED": "#3B82F6",
                "REMOVED": "#EF4444", "MATCH": "#6B7280"
            }
            bc = badge_colors.get(p.change_type, "#6B7280")

            preview = ""
            if p.added_text:
                preview = f"<div style='color: #22C55E; font-size: 12px;'>+ {html_escape(p.added_text[0][:120])}...</div>"
            elif p.removed_text:
                preview = f"<div style='color: #EF4444; font-size: 12px;'>- {html_escape(p.removed_text[0][:120])}...</div>"

            safe_title = html_escape(p.title or "(no title)")
            safe_url = html_escape(p.url)
            safe_domain = html_escape(p.domain)
            safe_page_type = html_escape(p.page_type)
            safe_language = html_escape(p.language)

            html.append(f"""
            <div style="border-left: 3px solid {bc}; padding: 8px 12px; margin: 8px 0; background: #F8FAFC;">
                <span style="background: {bc}; color: white; padding: 1px 6px; border-radius: 3px; font-size: 11px; font-weight: bold;">{p.change_type}</span>
                <strong style="margin-left: 6px;">{safe_title}</strong>
                <div style="font-size: 12px; color: #6B7280; margin-top: 4px;">
                    <a href="{safe_url}" style="color: #3B82F6;">{safe_url}</a>
                </div>
                <div style="font-size: 11px; color: #9CA3AF; margin-top: 2px;">
                    {safe_domain} | {safe_page_type} | {safe_language}
                </div>
                {preview}
            </div>
            """)

        if len(items) > 15:
            html.append(f"<p style='color: #9CA3AF; font-size: 12px;'>... and {len(items) - 15} more</p>")

    html.append("""
    <hr style="border: 1px solid #E5E7EB; margin-top: 30px;">
    <p style="color: #9CA3AF; font-size: 11px; text-align: center;">
        Firehose Daily Digest — taxesforexpats.com
    </p>
    </body></html>
    """)

    return "".join(html)


# ── Delivery ───────────────────────────────────────────────────

def send_to_slack(digest: str):
    if not SLACK_WEBHOOK_URL:
        return
    if len(digest) > 39000:
        digest = digest[:39000] + "\n\n... (truncated)"
    payload = json.dumps({"text": f"```{digest}```"}).encode()
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"  Slack: sent ({resp.status})")
    except Exception as e:
        print(f"  Slack: failed -- {e}", file=sys.stderr)


def send_via_resend(html_digest: str, event_count: int):
    if not RESEND_API_KEY or not RESEND_TO:
        return
    now = datetime.now(timezone.utc)
    subject = f"Firehose SEO Digest — {now:%b %d} — {event_count} events"
    payload = json.dumps({
        "from": RESEND_FROM,
        "to": [RESEND_TO],
        "subject": subject,
        "html": html_digest,
    }).encode()
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=payload,
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            print(f"  Resend: sent (id: {result.get('id', 'unknown')})")
    except Exception as e:
        print(f"  Resend: failed -- {e}", file=sys.stderr)


def save_to_file(digest: str, html_digest: str):
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")

    txt_path = f"digest_{date_str}.txt"
    with open(txt_path, "w") as f:
        f.write(digest)
    print(f"  Saved: {txt_path}")

    html_path = f"digest_{date_str}.html"
    with open(html_path, "w") as f:
        f.write(html_digest)
    print(f"  Saved: {html_path}")


# ── Rules ──────────────────────────────────────────────────────

EXAMPLE_RULES = [
    # --- Competitor Content Monitoring (7 rules) ---
    {"value": 'domain:"brighttax.com" AND language:"en" AND recent:24h', "tag": "comp-content"},
    {"value": 'domain:"greenbacktaxservices.com" AND language:"en" AND recent:24h', "tag": "comp-content"},
    {"value": 'domain:"1040abroad.com" AND language:"en" AND recent:24h', "tag": "comp-content"},
    {"value": 'domain:"hrblock.com" AND language:"en" AND recent:24h', "tag": "comp-content"},
    {"value": 'domain:"myexpattaxes.com" AND language:"en" AND recent:24h', "tag": "comp-content"},
    {"value": 'domain:"expattaxprofessionals.com" AND language:"en" AND recent:24h', "tag": "comp-content"},
    {"value": 'domain:"expattaxonline.com" AND language:"en" AND recent:24h', "tag": "comp-content"},
    # --- Brand & Citation Monitoring (6 rules) ---
    {"value": 'added:"Taxes for Expats" AND language:"en"', "tag": "brand"},
    {"value": 'added_anchor:"Taxes for Expats" AND language:"en"', "tag": "brand"},
    {"value": 'added:"taxesforexpats.com" AND language:"en"', "tag": "brand"},
    {"value": 'added:"TFX" AND added:"expat tax" AND language:"en"', "tag": "brand"},
    {"value": 'added:"according to" AND added:"Taxes for Expats" AND language:"en"', "tag": "brand"},
    {"value": 'removed:"Taxes for Expats" AND language:"en"', "tag": "brand"},
    # --- Own Site (1 rule) ---
    {"value": 'domain:"taxesforexpats.com" AND recent:24h', "tag": "own-site", "quality": False},
]


def setup_rules():
    print("Setting up rules...")
    existing = api_request("/v1/rules")
    existing_values = {r["value"] for r in existing.get("data", [])}
    existing_count = len(existing_values)

    created = 0
    for rule in EXAMPLE_RULES:
        if rule["value"] in existing_values:
            print(f"  [SKIP] Already exists: {rule['tag']} -- {rule['value'][:60]}")
            continue
        try:
            result = api_request("/v1/rules", method="POST", body=rule)
            rid = result.get("data", {}).get("id", "?")
            print(f"  [OK]   Rule {rid}: {rule['tag']} -- {rule['value'][:60]}...")
            created += 1
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  [FAIL] {rule['tag']}: {e.code} -- {body}", file=sys.stderr)

    print(f"\nCreated {created} new rules. Total active: {existing_count + created}/25")


# ── Main ───────────────────────────────────────────────────────

def main():
    if not TAP_TOKEN:
        print("Error: Set FIREHOSE_TAP_TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        setup_rules()
        return

    print(f"Firehose Daily Digest — taxesforexpats.com")
    print(f"  Window: {SINCE}  |  Limit: {LIMIT}  |  Timeout: {TIMEOUT}s")
    print()

    print("1. Fetching rules...")
    rules = fetch_rules()
    print(f"   Found {len(rules)} rules")
    for rid, rule in rules.items():
        print(f"   [{rule.get('tag', '?')}] {rule['value'][:70]}")
    print()

    print("2. Consuming SSE stream...")
    pages = consume_stream(rules)
    print(f"   Received {len(pages)} events")
    print()

    if not pages:
        print(f"No events matched in the last {SINCE}. Check your rules.")
        return

    print("3. Building digest...")
    digest = build_digest(pages)
    html_digest = build_html_digest(pages)

    print("4. Delivering...")
    save_to_file(digest, html_digest)
    send_to_slack(digest)
    send_via_resend(html_digest, len(pages))

    print()
    print(digest)


if __name__ == "__main__":
    main()
