import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "db"
DB_PATH = DB_DIR / "app.db"


def get_connection():
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_connection() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            file_name TEXT NOT NULL
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS view_columns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            view_id INTEGER NOT NULL,
            source_column_name TEXT NOT NULL,
            display_name TEXT NOT NULL,
            is_visible INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(view_id) REFERENCES views(id)
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS computed_columns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            view_id INTEGER NOT NULL,
            column_name TEXT NOT NULL,
            formula TEXT NOT NULL,
            is_visible INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 1000,
            FOREIGN KEY(view_id) REFERENCES views(id)
        )
        """)
        conn.commit()


def get_all_views():
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, name, slug, file_name
            FROM views
            ORDER BY id DESC
        """).fetchall()
        return [dict(row) for row in rows]


def get_view_by_id(view_id: int):
    with get_connection() as conn:
        row = conn.execute("""
            SELECT id, name, slug, file_name
            FROM views
            WHERE id = ?
        """, (view_id,)).fetchone()
        return dict(row) if row else None


def get_view_by_slug(slug: str):
    with get_connection() as conn:
        row = conn.execute("""
            SELECT id, name, slug, file_name
            FROM views
            WHERE slug = ?
        """, (slug,)).fetchone()
        return dict(row) if row else None


def create_view(name: str, slug: str, file_name: str):
    with get_connection() as conn:
        cur = conn.execute("""
            INSERT INTO views (name, slug, file_name)
            VALUES (?, ?, ?)
        """, (name, slug, file_name))
        conn.commit()
        return cur.lastrowid


def get_view_columns(view_id: int):
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, view_id, source_column_name, display_name, is_visible, sort_order
            FROM view_columns
            WHERE view_id = ?
            ORDER BY sort_order, id
        """, (view_id,)).fetchall()
        return [dict(row) for row in rows]


def add_view_column(view_id: int, source_column_name: str, display_name: str, is_visible: int, sort_order: int):
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO view_columns (view_id, source_column_name, display_name, is_visible, sort_order)
            VALUES (?, ?, ?, ?, ?)
        """, (view_id, source_column_name, display_name, is_visible, sort_order))
        conn.commit()


def update_view_column(column_id: int, display_name: str, is_visible: int, sort_order: int):
    with get_connection() as conn:
        conn.execute("""
            UPDATE view_columns
            SET display_name = ?, is_visible = ?, sort_order = ?
            WHERE id = ?
        """, (display_name, is_visible, sort_order, column_id))
        conn.commit()


def delete_view_column(column_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM view_columns WHERE id = ?", (column_id,))
        conn.commit()


def get_computed_columns(view_id: int):
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, view_id, column_name, formula, is_visible, sort_order
            FROM computed_columns
            WHERE view_id = ?
            ORDER BY sort_order, id
        """, (view_id,)).fetchall()
        return [dict(row) for row in rows]


def add_computed_column(view_id: int, column_name: str, formula: str, is_visible: int = 1, sort_order: int = 1000):
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO computed_columns (view_id, column_name, formula, is_visible, sort_order)
            VALUES (?, ?, ?, ?, ?)
        """, (view_id, column_name, formula, is_visible, sort_order))
        conn.commit()


def delete_computed_column(column_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM computed_columns WHERE id = ?", (column_id,))
        conn.commit()

def update_computed_column(column_id: int, column_name: str, formula: str, is_visible: int, sort_order: int):
    with get_connection() as conn:
        conn.execute("""
            UPDATE computed_columns
            SET column_name = ?, formula = ?, is_visible = ?, sort_order = ?
            WHERE id = ?
        """, (column_name, formula, is_visible, sort_order, column_id))
        conn.commit()        