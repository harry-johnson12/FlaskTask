"""
Lightweight SQLite helpers for the store.
The goal is to keep the schema easy to read while covering core store tables.
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Optional

# Store the database alongside the app for easy access.
DB_PATH = Path(__file__).with_name("store.db")


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    """Return a connection with row access by column name and foreign keys on."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
        conn.commit()
    finally:
        conn.close()


def _insert_and_return_id(conn: sqlite3.Connection, query: str, params: tuple[object, ...]) -> int:
    """Execute an INSERT statement and hand back the created row id."""
    cursor = conn.execute(query, params)
    new_id = cursor.lastrowid
    if new_id is None:
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    if new_id is None:
        raise RuntimeError("Could not determine the new row id.")
    return int(new_id)


def init_db() -> None:
    """Create the core store tables if they are missing."""
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                price REAL NOT NULL,
                sku TEXT UNIQUE,
                inventory_count INTEGER DEFAULT 0,
                image_path TEXT
            );

            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                order_date TEXT DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL DEFAULT 'pending',
                total REAL NOT NULL DEFAULT 0,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                unit_price REAL NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
            );
            """
        )
        _ensure_product_columns(conn)

    seed_data()


def insert_product(
    name: str,
    description: str,
    price: float,
    sku: Optional[str] = None,
    inventory_count: int = 0,
    image_path: Optional[str] = None,
) -> None:
    """Persist a new product using a simple parameterized query."""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO products (name, description, price, sku, inventory_count, image_path)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name, description, price, sku, inventory_count, image_path),
        )


def fetch_products(
    *,
    search: Optional[str] = None,
    stock_filter: Optional[str] = None,
    sort: str = "newest",
) -> Iterable[Mapping[str, object]]:
    """Return products ordered according to the requested sort and filters."""
    conditions: list[str] = []
    params: list[object] = []

    if search:
        like_term = f"%{search.lower()}%"
        conditions.append(
            "(LOWER(name) LIKE ? OR LOWER(description) LIKE ? OR LOWER(COALESCE(sku, '')) LIKE ?)"
        )
        params.extend([like_term, like_term, like_term])

    stock_map = {
        "in": "inventory_count > 0",
        "low": "inventory_count > 0 AND inventory_count <= 10",
        "out": "inventory_count <= 0",
    }
    stock_clause = stock_map.get((stock_filter or "").lower())
    if stock_clause:
        conditions.append(stock_clause)

    order_by_map = {
        "newest": "id DESC",
        "oldest": "id ASC",
        "price_low": "price ASC, id DESC",
        "price_high": "price DESC, id DESC",
        "inventory_low": "inventory_count ASC, id DESC",
        "inventory_high": "inventory_count DESC, id DESC",
        "name_az": "LOWER(name) ASC, id DESC",
        "name_za": "LOWER(name) DESC, id DESC",
    }
    order_clause = order_by_map.get(sort, order_by_map["newest"])

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, name, description, price, sku, inventory_count, image_path
            FROM products
            {where_clause}
            ORDER BY {order_clause}
            """,
            params,
        )
        return [dict(row) for row in rows]


def fetch_products_by_ids(product_ids: Iterable[int]) -> list[Mapping[str, object]]:
    """Return products for the provided ids preserving the original order."""
    seen: set[int] = set()
    ordered_ids: list[int] = []
    for product_id in product_ids:
        try:
            pid = int(product_id)
        except (TypeError, ValueError):
            continue
        if pid not in seen:
            ordered_ids.append(pid)
            seen.add(pid)

    if not ordered_ids:
        return []

    placeholders = ", ".join("?" for _ in ordered_ids)
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, name, description, price, sku, inventory_count, image_path
            FROM products
            WHERE id IN ({placeholders})
            """,
            ordered_ids,
        )
        lookup = {int(row["id"]): dict(row) for row in rows}

    return [lookup[pid] for pid in ordered_ids if pid in lookup]


def insert_customer(first_name: str, last_name: str, email: str) -> int:
    """Create a customer and hand back the new id."""
    with get_connection() as conn:
        return _insert_and_return_id(
            conn,
            """
            INSERT INTO customers (first_name, last_name, email)
            VALUES (?, ?, ?)
            """,
            (first_name, last_name, email),
        )


def insert_order(customer_id: int, status: str = "pending") -> int:
    """Start an order for a customer."""
    with get_connection() as conn:
        return _insert_and_return_id(
            conn,
            """
            INSERT INTO orders (customer_id, status)
            VALUES (?, ?)
            """,
            (customer_id, status),
        )


def add_order_item(order_id: int, product_id: int, quantity: int, unit_price: float) -> None:
    """Attach a product to an order and maintain the order total."""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO order_items (order_id, product_id, quantity, unit_price)
            VALUES (?, ?, ?, ?)
            """,
            (order_id, product_id, quantity, unit_price),
        )
        conn.execute(
            """
            UPDATE orders
            SET total = (
                SELECT COALESCE(SUM(quantity * unit_price), 0)
                FROM order_items
                WHERE order_id = ?
            )
            WHERE id = ?
            """,
            (order_id, order_id),
        )


def delete_product(product_id: int) -> None:
    """Remove a product and any orphaned order items."""
    with get_connection() as conn:
        conn.execute("DELETE FROM products WHERE id = ?", (product_id,))


def fetch_customers() -> Iterable[Mapping[str, object]]:
    """Return customers ordered by newest first."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, first_name, last_name, email, created_at
            FROM customers
            ORDER BY id DESC
            """
        )
        return list(rows)


def delete_customer(customer_id: int) -> None:
    """Remove a customer which cascades to their orders."""
    with get_connection() as conn:
        conn.execute("DELETE FROM customers WHERE id = ?", (customer_id,))


def seed_data() -> None:
    """Populate a few sample products so the UI has content."""
    samples = [
        {
            "name": "Glide Serum",
            "description": "Featherlight silicone blend that locks in comfort for long runs.",
            "price": 24.0,
            "sku": "CE-GS-01",
            "inventory": 36,
            "image_path": None,
        },
        {
            "name": "Recalibrate Balm",
            "description": "Post-run recovery balm with cooling botanicals and ceramides.",
            "price": 28.0,
            "sku": "CE-RB-02",
            "inventory": 22,
            "image_path": None,
        },
        {
            "name": "Pulse Wipes",
            "description": "Single-use wipes that reset skin pH and remove salt build-up.",
            "price": 14.0,
            "sku": "CE-PW-03",
            "inventory": 50,
            "image_path": None,
        },
        {
            "name": "Aero Shield Stick",
            "description": "Wind-resistant barrier stick that prevents raw spots on exposed skin.",
            "price": 18.0,
            "sku": "CE-AS-04",
            "inventory": 31,
            "image_path": None,
        },
        {
            "name": "Afterburn Restore Cream",
            "description": "Ultra-hydrating cream with niacinamide and oat peptides for post-race relief.",
            "price": 32.0,
            "sku": "CE-AR-05",
            "inventory": 27,
            "image_path": None,
        },
        {
            "name": "Night Shift Recovery Oil",
            "description": "Overnight recovery oil infused with seabuckthorn to repair micro-abrasions.",
            "price": 35.0,
            "sku": "CE-NS-06",
            "inventory": 18,
            "image_path": None,
        },
        {
            "name": "Stride Guard Powder",
            "description": "Talc-free powder that keeps moisture in check during peak humidity miles.",
            "price": 16.0,
            "sku": "CE-SG-07",
            "inventory": 40,
            "image_path": None,
        },
        {
            "name": "Base Layer Shield",
            "description": "Spray-on base layer formula designed to prevent seam irritation under kits.",
            "price": 26.0,
            "sku": "CE-BS-08",
            "inventory": 34,
            "image_path": None,
        },
        {
            "name": "Trail Reset Mist",
            "description": "On-the-go mist that cools and neutralises salt after technical climbs.",
            "price": 19.0,
            "sku": "CE-TR-09",
            "inventory": 45,
            "image_path": None,
        },
        {
            "name": "Ultra Repair Duo",
            "description": "Two-step kit combining Glide Serum and Night Shift Recovery Oil for race weekends.",
            "price": 56.0,
            "sku": "CE-UR-10",
            "inventory": 12,
            "image_path": None,
        },
    ]

    with get_connection() as conn:
        current_version = conn.execute("PRAGMA user_version").fetchone()[0]
        target_version = 2

        needs_refresh = current_version < target_version
        if not needs_refresh:
            product_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            needs_refresh = product_count == 0

        if needs_refresh:
            conn.execute("DELETE FROM products")
            for product in samples:
                conn.execute(
                    """
                    INSERT INTO products (name, description, price, sku, inventory_count, image_path)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        product["name"],
                        product["description"],
                        product["price"],
                        product["sku"],
                        product["inventory"],
                        product["image_path"],
                    ),
                )
            conn.execute(f"PRAGMA user_version = {target_version}")

        # Seed a couple of customers so analytics look alive.
        customer_count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        if customer_count == 0:
            conn.executemany(
                """
                INSERT INTO customers (first_name, last_name, email)
                VALUES (?, ?, ?)
                """,
                [
                    ("Jamie", "Rivera", "jamie@example.com"),
                    ("Taylor", "Bennett", "taylor@example.com"),
                ],
            )


def get_product(product_id: int) -> Optional[Mapping[str, object]]:
    """Return a single product or None when not found."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, name, description, price, sku, inventory_count, image_path
            FROM products
            WHERE id = ?
            """,
            (product_id,),
        ).fetchone()
        return dict(row) if row else None


def update_product(
    product_id: int,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    price: Optional[float] = None,
    sku: Optional[str] = None,
    inventory_count: Optional[int] = None,
    image_path: Optional[str] = None,
) -> None:
    """Update a product with provided fields."""
    fields = []
    values: list[object] = []

    def _append(column: str, value: Optional[object]) -> None:
        if value is not None:
            fields.append(f"{column} = ?")
            values.append(value)

    _append("name", name)
    _append("description", description)
    _append("price", price)
    _append("sku", sku)
    _append("inventory_count", inventory_count)
    _append("image_path", image_path)

    if not fields:
        return

    with get_connection() as conn:
        conn.execute(
            f"UPDATE products SET {', '.join(fields)} WHERE id = ?",
            (*values, product_id),
        )


def _ensure_product_columns(conn: sqlite3.Connection) -> None:
    """Add newer columns to the products table when upgrading."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(products)")}
    if "image_path" not in columns:
        conn.execute("ALTER TABLE products ADD COLUMN image_path TEXT")
