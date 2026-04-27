#!/usr/bin/env python3
"""
Firehose Daily SEO Digest — taxesforexpats.com
"""

import json
import os
import sys
import smtplib
import urllib.request
import urllib.error
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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

# Gmail SMTP
GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_PASSWORD = os.environ.get("GMAIL_PASSWORD", "")
EMAIL_TO = os.environ.get("EMAIL_TO", "")


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
        "link-opps":    "#8B5CF6",
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


def send_via_gmail(html_digest: str, event_count: int):
    if not GMAIL_USER or not GMAIL_PASSWORD or not EMAIL_TO:
        print("  Email: skipped (GMAIL_USER / GMAIL_PASSWORD / EMAIL_TO not set)")
        return

    now = datetime.now(timezone.utc)
    subject = f"Firehose SEO Digest — {now:%b %d} — {event_count} events"
    recipients = [addr.strip() for addr in EMAIL_TO.split(",") if addr.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_digest, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, recipients, msg.as_string())
        print(f"  Email: sent to {recipients}")
    except Exception as e:
        print(f"  Email: failed -- {e}", file=sys.stderr)


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
    # --- Link Opportunity Discovery (4 rules) ---
    {"value": 'added_anchor:"expat tax preparation" AND page_type:"/Article" AND language:"en"', "tag": "link-opps"},
    {"value": 'added_anchor:"expat tax filing" AND page_type:"/Article" AND language:"en"', "tag": "link-opps"},
    {"value": 'added_anchor:"US expat taxes" AND page_type:"/Article" AND language:"en"', "tag": "link-opps"},
    {"value": 'added_anchor:"expat tax" AND page_type:"/Article" AND language:"en"', "tag": "link-opps"},
    # --- Own Site (1 rule) ---
    {"value": 'domain:"taxesforexpats.com" AND recent:24h', "tag": "own-site"},
]

OBSOLETE_RULE_VALUES = {
    'added_anchor:"FBAR" AND page_type:"/Article" AND language:"en"',
}


def setup_rules():
    print("Setting up rules...")
    existing_data = api_request("/v1/rules").get("data", [])
    existing_by_value = {r["value"]: r for r in existing_data}

    deleted = 0
    for rule in existing_data:
        if rule["value"] in OBSOLETE_RULE_VALUES:
            rid = rule["id"]
            try:
                api_request(f"/v1/rules/{rid}", method="DELETE")
                print(f"  [DEL]  Removed obsolete rule {rid}: {rule['value'][:60]}")
                del existing_by_value[rule["value"]]
                deleted += 1
            except urllib.error.HTTPError as e:
                body = e.read().decode()
                print(f"  [FAIL] Delete {rid}: {e.code} -- {body}", file=sys.stderr)

    created = 0
    for rule in EXAMPLE_RULES:
        if rule["value"] in existing_by_value:
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

    total = len(existing_by_value) - deleted + created
    print(f"\nDeleted {deleted} obsolete rules. Created {created} new rules. Total active: {total}/25")


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
    send_via_gmail(html_digest, len(pages))

    print()
    print(digest)


if __name__ == "__main__":
    main()
