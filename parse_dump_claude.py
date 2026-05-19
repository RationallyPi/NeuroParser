import os
import sys
import bz2
import re
import psycopg2
import mwxml
import mwparserfromhell
from dotenv import load_dotenv


load_dotenv()

# ─── Configuration ────────────────────────────────────────────────
DB_URL      = os.getenv("DATABASE_URL")
BATCH_SIZE  = 500    # Safe window for Supabase free-tier connection limits
MAX_ARTICLES = 30000 # Stop after this many articles — free tier safe
# ──────────────────────────────────────────────────────────────────


def get_complexity_tier(char_count: int, template_count: int) -> str:
    """
    Maps raw article features to a routing-ready complexity tier.
    Used downstream by the RF model and load balancer.
    """
    score = char_count + (template_count * 200)
    if score < 5000:
        return "light"
    if score < 30000:
        return "medium"
    return "heavy"


def flush_batch(cursor, conn, batch: list):
    """
    Executes a batched upsert to Supabase.
    ON CONFLICT makes this safely resumable — restart anytime without duplicates.
    """
    cursor.executemany("""
        INSERT INTO articles (
            page_id, title, char_count, template_count,
            heading_count, link_count, table_count, complexity_tier
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (page_id) DO UPDATE SET
            title           = EXCLUDED.title,
            char_count      = EXCLUDED.char_count,
            template_count  = EXCLUDED.template_count,
            heading_count   = EXCLUDED.heading_count,
            link_count      = EXCLUDED.link_count,
            table_count     = EXCLUDED.table_count,
            complexity_tier = EXCLUDED.complexity_tier
    """, batch)
    conn.commit()


def init_schema(cursor, conn):
    """
    Creates the articles table if it doesn't exist.
    Raw wikitext is intentionally excluded — too large for free tier.
    complexity_tier maps directly to load balancer routing logic.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            page_id         INT PRIMARY KEY,
            title           TEXT,
            char_count      INT,
            template_count  INT,
            heading_count   INT,
            link_count      INT,
            table_count     INT,
            complexity_tier TEXT
        )
    """)
    conn.commit()
    print("✅ Schema ready.")


def extract_features(raw_text: str) -> dict:
    """
    Extracts structural complexity features from raw wikitext.
    Falls back to regex if the AST parser chokes on malformed markup.
    """
    try:
        wikicode = mwparserfromhell.parse(raw_text)

        template_count = len(wikicode.filter_templates())
        heading_count  = len(wikicode.filter_headings())
        link_count     = (
            len(wikicode.filter_wikilinks()) +
            len(wikicode.filter_external_links())
        )
        table_count = len(
            wikicode.filter_tags(matches=lambda tag: tag.tag == "table")
        )

    except Exception:
        # Regex fallback — some Wikipedia markup genuinely breaks the AST builder
        template_count = raw_text.count("{{")
        heading_count  = len(re.findall(r'^==+.+?==+', raw_text, re.MULTILINE))
        link_count     = raw_text.count("[[") + raw_text.count("http")
        table_count    = raw_text.count("{|")

    return {
        "template_count": template_count,
        "heading_count":  heading_count,
        "link_count":     link_count,
        "table_count":    table_count,
    }


def parse_and_upload(dump_path: str):
    if not DB_URL:
        print("❌ DATABASE_URL environment variable is not set.")
        print("   Run: export DATABASE_URL='your_supabase_connection_string'")
        sys.exit(1)

    if not os.path.exists(dump_path):
        print(f"❌ Dump file not found: {dump_path}")
        sys.exit(1)

    print(f"📡 Connecting to Supabase...")

    # Context manager ensures connection closes cleanly even on crash
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cursor:

            init_schema(cursor, conn)

            print(f"🚀 Streaming decompression: {dump_path}")
            print(f"   Limit: {MAX_ARTICLES:,} articles\n")

            with bz2.open(dump_path, "rb") as compressed_file:
                dump = mwxml.Dump.from_file(compressed_file)

                batch           = []
                processed_count = 0
                skipped_count   = 0

                for page in dump:

                    # Hard stop — keeps us within Supabase free tier
                    if processed_count >= MAX_ARTICLES:
                        print(f"\n⏹  Reached {MAX_ARTICLES:,} article limit.")
                        break

                    # Skip non-article namespaces and redirects
                    if page.namespace != 0 or page.redirect:
                        skipped_count += 1
                        continue

                    # Get the latest revision — dumps are oldest-first,
                    # so we consume the full iterator and take the last one
                    latest_revision = None
                    for revision in page:
                        latest_revision = revision

                    if latest_revision is None:
                        continue

                    raw_text   = latest_revision.text or ""
                    char_count = len(raw_text)

                    features = extract_features(raw_text)
                    tier     = get_complexity_tier(char_count, features["template_count"])

                    row = (
                        int(page.id),
                        str(page.title),
                        int(char_count),
                        int(features["template_count"]),
                        int(features["heading_count"]),
                        int(features["link_count"]),
                        int(features["table_count"]),
                        tier,
                    )
                    batch.append(row)

                    if len(batch) >= BATCH_SIZE:
                        flush_batch(cursor, conn, batch)
                        processed_count += len(batch)
                        batch = []
                        print(
                            f"   ✓ {processed_count:,} / {MAX_ARTICLES:,} articles uploaded...",
                            end="\r"
                        )

                # Flush any remaining rows after the loop ends
                if batch:
                    flush_batch(cursor, conn, batch)
                    processed_count += len(batch)

    print(f"\n\n🎉 Done.")
    print(f"   Articles uploaded : {processed_count:,}")
    print(f"   Pages skipped     : {skipped_count:,}  (redirects, talk pages, etc.)")
    print(f"\nComplexity breakdown query:")
    print(f"   SELECT complexity_tier, COUNT(*) FROM articles GROUP BY complexity_tier;")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Missing dump file path.")
        print("   Usage: python parse_dump.py <path_to_dump.xml.bz2>")
        sys.exit(1)

    parse_and_upload(sys.argv[1])