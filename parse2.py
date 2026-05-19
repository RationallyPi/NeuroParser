import os
import sys
import bz2
import re
import psycopg2
from dotenv import load_dotenv
import mwxml
import mwparserfromhell
from datetime import datetime, timezone

# ─── Configuration ────────────────────────────────────────────────
load_dotenv()
DB_URL       = os.getenv("DATABASE_URL")
BATCH_SIZE   = 100   # Reduced — raw_wikitext rows are large
MAX_ARTICLES = 10000 # Free tier safe limit
# ──────────────────────────────────────────────────────────────────


# ─── Schema ───────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS wiki_pages (
    page_id            BIGINT PRIMARY KEY,
    title              TEXT NOT NULL,
    revision_id        BIGINT,
    revision_timestamp TIMESTAMP,
    raw_wikitext       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wiki_page_features (
    page_id                    BIGINT PRIMARY KEY REFERENCES wiki_pages(page_id),
    wikitext_length_bytes      INT NOT NULL,
    template_count             INT NOT NULL,
    image_count                INT NOT NULL,
    reference_count            INT NOT NULL,
    heading_count              INT NOT NULL,
    internal_link_count        INT NOT NULL,
    external_link_count        INT NOT NULL,
    category_count             INT NOT NULL,
    table_tag_count            INT,
    paragraph_tag_count        INT,
    rendered_html_length_bytes INT,
    render_expansion_ratio     FLOAT,
    html_tag_count             INT
);

CREATE TABLE IF NOT EXISTS wiki_page_labels (
    page_id           BIGINT PRIMARY KEY REFERENCES wiki_pages(page_id),
    avg_response_time FLOAT,
    is_slow           SMALLINT CHECK (is_slow IN (0, 1))
);
"""

# ──────────────────────────────────────────────────────────────────


def init_schema(cursor, conn):
    cursor.execute(SCHEMA_SQL)
    conn.commit()
    print("✅ Schema ready — 3 tables initialized.")


# ─── Feature Extraction ───────────────────────────────────────────

def extract_features(raw_text: str) -> dict:
    """
    Extracts all columns for wiki_page_features.
    Attempts AST parsing first, falls back to regex on malformed markup.

    rendered_html_length_bytes, render_expansion_ratio, html_tag_count,
    and paragraph_tag_count are approximated from wikitext structure
    since mwparserfromhell does not do full HTML rendering.
    """
    wikitext_length_bytes = len(raw_text.encode("utf-8"))

    try:
        wikicode = mwparserfromhell.parse(raw_text)

        all_links      = wikicode.filter_wikilinks()
        all_tags       = wikicode.filter_tags()

        template_count      = len(wikicode.filter_templates())
        heading_count       = len(wikicode.filter_headings())
        internal_link_count = len(all_links)
        external_link_count = len(wikicode.filter_external_links())
        html_tag_count      = len(all_tags)

        # Images — wikilinks whose title starts with File: or Image:
        image_count = len([
            l for l in all_links
            if str(l.title).startswith(("File:", "Image:", "file:", "image:"))
        ])

        # Categories — wikilinks starting with Category:
        category_count = len([
            l for l in all_links
            if str(l.title).startswith(("Category:", "category:"))
        ])

        # References — <ref> tags
        reference_count = len([
            t for t in all_tags
            if str(t.tag).lower() == "ref"
        ])

        # Tables — <table> tags
        table_tag_count = len([
            t for t in all_tags
            if str(t.tag).lower() == "table"
        ])

        # Paragraphs — double newlines in stripped plain text
        stripped            = wikicode.strip_code()
        paragraph_tag_count = stripped.count("\n\n")

        # Rendered length approximation — stripped plain text byte size
        rendered_html_length_bytes = len(stripped.encode("utf-8"))

        # Expansion ratio — wikitext bytes vs plain text bytes
        render_expansion_ratio = (
            round(wikitext_length_bytes / rendered_html_length_bytes, 4)
            if rendered_html_length_bytes > 0 else None
        )

    except Exception:
        # Regex fallback — for malformed markup that breaks the AST
        template_count      = raw_text.count("{{")
        heading_count       = len(re.findall(r'^==+.+?==+', raw_text, re.MULTILINE))
        internal_link_count = raw_text.count("[[")
        external_link_count = len(re.findall(r'https?://', raw_text))
        image_count         = len(re.findall(r'\[\[(File|Image):', raw_text, re.IGNORECASE))
        category_count      = len(re.findall(r'\[\[Category:', raw_text, re.IGNORECASE))
        reference_count     = raw_text.count("<ref")
        table_tag_count     = raw_text.count("{|")
        paragraph_tag_count = raw_text.count("\n\n")
        html_tag_count      = len(re.findall(r'<[^>]+>', raw_text))

        # Cannot approximate these reliably without AST
        rendered_html_length_bytes = None
        render_expansion_ratio     = None

    return {
        "wikitext_length_bytes":      wikitext_length_bytes,
        "template_count":             template_count,
        "image_count":                image_count,
        "reference_count":            reference_count,
        "heading_count":              heading_count,
        "internal_link_count":        internal_link_count,
        "external_link_count":        external_link_count,
        "category_count":             category_count,
        "table_tag_count":            table_tag_count,
        "paragraph_tag_count":        paragraph_tag_count,
        "rendered_html_length_bytes": rendered_html_length_bytes,
        "render_expansion_ratio":     render_expansion_ratio,
        "html_tag_count":             html_tag_count,
    }


# ─── Batch Flushing ───────────────────────────────────────────────

def flush_batches(cursor, conn, pages: list, features: list):
    """
    Upserts wiki_pages first (parent table), then wiki_page_features (child).
    Order matters — features has a FK reference to pages.

    wiki_page_labels is intentionally left empty here.
    It gets populated later by your load balancer after it measures
    actual response times per article payload.
    """

    # 1. Parent — wiki_pages
    cursor.executemany("""
        INSERT INTO wiki_pages (
            page_id, title, revision_id, revision_timestamp, raw_wikitext
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (page_id) DO UPDATE SET
            title              = EXCLUDED.title,
            revision_id        = EXCLUDED.revision_id,
            revision_timestamp = EXCLUDED.revision_timestamp,
            raw_wikitext       = EXCLUDED.raw_wikitext
    """, pages)

    # 2. Child — wiki_page_features
    cursor.executemany("""
        INSERT INTO wiki_page_features (
            page_id, wikitext_length_bytes, template_count, image_count,
            reference_count, heading_count, internal_link_count,
            external_link_count, category_count, table_tag_count,
            paragraph_tag_count, rendered_html_length_bytes,
            render_expansion_ratio, html_tag_count
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (page_id) DO UPDATE SET
            wikitext_length_bytes      = EXCLUDED.wikitext_length_bytes,
            template_count             = EXCLUDED.template_count,
            image_count                = EXCLUDED.image_count,
            reference_count            = EXCLUDED.reference_count,
            heading_count              = EXCLUDED.heading_count,
            internal_link_count        = EXCLUDED.internal_link_count,
            external_link_count        = EXCLUDED.external_link_count,
            category_count             = EXCLUDED.category_count,
            table_tag_count            = EXCLUDED.table_tag_count,
            paragraph_tag_count        = EXCLUDED.paragraph_tag_count,
            rendered_html_length_bytes = EXCLUDED.rendered_html_length_bytes,
            render_expansion_ratio     = EXCLUDED.render_expansion_ratio,
            html_tag_count             = EXCLUDED.html_tag_count
    """, features)

    conn.commit()


# ─── Main ─────────────────────────────────────────────────────────

def parse_and_upload(dump_path: str):
    if not DB_URL:
        print("❌ DATABASE_URL is not set.")
        print("   Run: source .env")
        sys.exit(1)

    if not os.path.exists(dump_path):
        print(f"❌ Dump file not found: {dump_path}")
        sys.exit(1)

    # Connection test before streaming — fail fast before processing starts
    print("📡 Testing Supabase connection...")
    try:
        test = psycopg2.connect(DB_URL)
        test.close()
        print("✅ Connected.\n")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:

            init_schema(cursor, conn)

            print(f"🚀 Streaming: {dump_path}")
            print(f"   Batch size   : {BATCH_SIZE} articles")
            print(f"   Article limit: {MAX_ARTICLES:,}\n")

            with bz2.open(dump_path, "rb") as f:
                dump = mwxml.Dump.from_file(f)

                pages_batch    = []
                features_batch = []
                processed      = 0
                skipped        = 0

                for page in dump:

                    if processed >= MAX_ARTICLES:
                        print(f"\n⏹  Reached {MAX_ARTICLES:,} article limit.")
                        break

                    # Skip non-article namespaces and redirects
                    if page.namespace != 0 or page.redirect:
                        skipped += 1
                        continue

                    # pages-articles dump has exactly one revision per page
                    iterator = iter(page)
                    revision = next(iterator, None)

                    if revision is None:
                        continue

                    raw_text = revision.text or ""

                    # Parse revision timestamp safely
                    rev_timestamp = None
                    if revision.timestamp:
                        try:
                            rev_timestamp = datetime.fromtimestamp(
                                revision.timestamp.unix,
                                tz=timezone.utc
                            ).replace(tzinfo=None)  # strip tz — Supabase TIMESTAMP is naive
                        except Exception:
                            rev_timestamp = None

                    # wiki_pages row
                    pages_batch.append((
                        int(page.id),
                        str(page.title),
                        int(revision.id) if revision.id else None,
                        rev_timestamp,
                        raw_text,
                    ))

                    # wiki_page_features row
                    f = extract_features(raw_text)
                    features_batch.append((
                        int(page.id),
                        f["wikitext_length_bytes"],
                        f["template_count"],
                        f["image_count"],
                        f["reference_count"],
                        f["heading_count"],
                        f["internal_link_count"],
                        f["external_link_count"],
                        f["category_count"],
                        f["table_tag_count"],
                        f["paragraph_tag_count"],
                        f["rendered_html_length_bytes"],
                        f["render_expansion_ratio"],
                        f["html_tag_count"],
                    ))

                    if len(pages_batch) >= BATCH_SIZE:
                        flush_batches(cursor, conn, pages_batch, features_batch)
                        processed      += len(pages_batch)
                        pages_batch     = []
                        features_batch  = []
                        print(
                            f"   ✓ {processed:,} / {MAX_ARTICLES:,} uploaded...",
                            end="\r"
                        )

                # Final residual flush
                if pages_batch:
                    flush_batches(cursor, conn, pages_batch, features_batch)
                    processed += len(pages_batch)

    print(f"\n\n🎉 Done.")
    print(f"   Articles uploaded : {processed:,}")
    print(f"   Pages skipped     : {skipped:,}  (redirects, namespaces)")
    print(f"""
── Next steps ───────────────────────────────────────────────

1. Verify row counts in Supabase:
   SELECT COUNT(*) FROM wiki_pages;
   SELECT COUNT(*) FROM wiki_page_features;

2. Check feature spread:
   SELECT
     MIN(wikitext_length_bytes), MAX(wikitext_length_bytes),
     AVG(template_count)::INT,  AVG(reference_count)::INT
   FROM wiki_page_features;

3. After load balancer runs, populate labels:
   INSERT INTO wiki_page_labels (page_id, avg_response_time, is_slow)
   SELECT
     page_id,
     avg_response_time,
     CASE WHEN avg_response_time > 200 THEN 1 ELSE 0 END
   FROM your_timing_results
   ON CONFLICT (page_id) DO NOTHING;
────────────────────────────────────────────────────────────
    """)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Missing dump file.")
        print("   Usage: python parse_dump.py <path_to_dump.xml.bz2>")
        sys.exit(1)

    parse_and_upload(sys.argv[1])