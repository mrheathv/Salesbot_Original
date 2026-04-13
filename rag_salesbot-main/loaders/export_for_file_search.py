"""
Export DuckDB tables to plain-text files suitable for upload to
OpenAI's File Search / vector store (which does not support CSV).

Each table is written to output_dir/<table_name>.txt as a sequence
of human-readable records, one blank line between rows.

Run from the rag_salesbot-main directory:
    python loaders/export_for_file_search.py
"""

from pathlib import Path
import duckdb

DB = Path("db/sales.duckdb")
OUTPUT_DIR = Path("data/file_search")

TABLES = [
    "accounts",
    "products",
    "interactions",
    "sales_pipeline",
    "sales_teams",
]


def row_to_text(columns: list[str], row: tuple) -> str:
    lines = [f"{col}: {val}" for col, val in zip(columns, row)]
    return "\n".join(lines)


def export_table(con: duckdb.DuckDBPyConnection, table: str, out_path: Path) -> int:
    result = con.execute(f"SELECT * FROM {table}").fetchall()
    columns = [desc[0] for desc in con.description]

    records = [row_to_text(columns, row) for row in result]
    out_path.write_text("\n\n".join(records), encoding="utf-8")
    return len(records)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB.as_posix(), read_only=True)

    for table in TABLES:
        out_path = OUTPUT_DIR / f"{table}.txt"
        count = export_table(con, table, out_path)
        print(f"  {table}: {count} records → {out_path}")

    con.close()
    print("Done. Upload the .txt files in data/file_search/ to your OpenAI vector store.")


if __name__ == "__main__":
    main()
