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
    """Populate a modern electronics-focused catalogue so the UI has content."""
    samples = [
        {
            "name": "PulseLink HDMI 2.1 Cable 2m",
            "description": "Certified Ultra High Speed cable engineered for 4K120 and 8K displays with dynamic HDR.",
            "price": 22.0,
            "sku": "CF-HDMI-21",
            "inventory": 72,
            "image_path": "https://images.unsplash.com/photo-1580894908361-967195033215?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "NanoMesh Dupont Jumper Set (120 pack)",
            "description": "Pre-crimped male and female Dupont jumpers in colour-coded harnesses for rapid prototyping.",
            "price": 14.5,
            "sku": "CF-JPR-120",
            "inventory": 180,
            "image_path": "https://images.unsplash.com/photo-1582719478250-c89cae4dc85b?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "VectorForge ATX Motherboard X790",
            "description": "Performance ATX board with DDR5 support, triple NVMe slots, and integrated Wi-Fi 7.",
            "price": 329.0,
            "sku": "CF-MBD-X790",
            "inventory": 26,
            "image_path": "https://images.unsplash.com/photo-1580894906472-00a37b6e5fcd?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "QuantumBlade NVMe SSD 2TB",
            "description": "PCIe Gen4 M.2 solid state drive delivering 7,000 MB/s sequential reads with onboard heatsink.",
            "price": 199.0,
            "sku": "CF-SSD-QB2",
            "inventory": 64,
            "image_path": "https://images.unsplash.com/photo-1517433456452-f9633a875f6f?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "Helios 850W Modular PSU (80+ Platinum)",
            "description": "Fully modular power supply with low ripple output, digital monitoring, and silent mode.",
            "price": 189.0,
            "sku": "CF-PSU-850P",
            "inventory": 41,
            "image_path": "https://images.unsplash.com/photo-1624704764770-10284c658023?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "AuroraFlex USB-C Hub Pro",
            "description": "Aluminium hub with Thunderbolt passthrough, dual 4K display outputs, and NVMe expansion bay.",
            "price": 129.0,
            "sku": "CF-HUB-AF9",
            "inventory": 58,
            "image_path": "https://images.unsplash.com/photo-1587202372775-98927a9d23ee?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "TitanEdge GPU Bracket (ARGB)",
            "description": "Adjustable support bracket with addressable lighting to eliminate GPU sag in tempered glass builds.",
            "price": 39.0,
            "sku": "CF-GPU-TED",
            "inventory": 97,
            "image_path": "https://images.unsplash.com/photo-1517336714731-489689fd1ca8?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "IonCore Thermal Paste X9",
            "description": "Nano-diamond thermal compound with low viscosity for high surface coverage and 12.5 W/mK conductivity.",
            "price": 11.0,
            "sku": "CF-THP-X9",
            "inventory": 240,
            "image_path": "https://images.unsplash.com/photo-1580894897249-4c37c03fed7a?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "MatrixLab Precision Screwdriver Set",
            "description": "40-bit magnetic driver kit with knurled grip handle for electronics disassembly and repair.",
            "price": 54.0,
            "sku": "CF-TLS-M40",
            "inventory": 88,
            "image_path": "https://images.unsplash.com/photo-1582719478250-63cb8f8ca68c?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "GridWave Wi-Fi 7 Router",
            "description": "Tri-band mesh-ready router with 10G WAN, OFDMA support, and quantum-resistant WPA4 firmware.",
            "price": 289.0,
            "sku": "CF-NET-GW7",
            "inventory": 35,
            "image_path": "https://images.unsplash.com/photo-1595433707802-6b2626ef093d?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "LumenStrip Addressable LED Kit",
            "description": "5m addressable RGB LED strip with adhesive backing, USB-C controller, and open API.",
            "price": 59.0,
            "sku": "CF-LIT-LMK",
            "inventory": 120,
            "image_path": "https://images.unsplash.com/photo-1541532713592-79a0317b6b77?auto=format&fit=crop&w=900&q=80",
        },
        {
            "name": "OptiMesh 140mm PWM Fan (2 pack)",
            "description": "Low vibration 140mm fans with fluid dynamic bearings and daisy-chainable PWM headers.",
            "price": 44.0,
            "sku": "CF-FAN-140P",
            "inventory": 105,
            "image_path": "https://images.unsplash.com/photo-1580894894513-541e068a0d0e?auto=format&fit=crop&w=900&q=80",
        },
    ]

    with get_connection() as conn:
        current_version = conn.execute("PRAGMA user_version").fetchone()[0]
        target_version = 3

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
