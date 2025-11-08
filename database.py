"""
Lightweight SQLite helpers for the store.
The goal is to keep the schema easy to read while covering core store tables.
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, Iterator, Mapping, Optional

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
                image_path TEXT,
                category TEXT NOT NULL DEFAULT 'General',
                seller_id INTEGER,
                FOREIGN KEY (seller_id) REFERENCES sellers(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                is_seller INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sellers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                store_name TEXT NOT NULL,
                description TEXT,
                contact_email TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                company TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                seller_id INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                total_amount REAL NOT NULL DEFAULT 0,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
                FOREIGN KEY (seller_id) REFERENCES sellers(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS user_cart_items (
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (user_id, product_id),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS product_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
                comment TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (product_id, user_id),
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS user_recent_products (
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                viewed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, product_id),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
            );
            """
        )
        _ensure_product_columns(conn)
        _ensure_user_columns(conn)

    seed_data()


def insert_product(
    name: str,
    description: str,
    price: float,
    sku: Optional[str] = None,
    inventory_count: int = 0,
    image_path: Optional[str] = None,
    category: str = "General",
    seller_id: Optional[int] = None,
) -> None:
    """Persist a new product using a simple parameterized query."""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO products (name, description, price, sku, inventory_count, image_path, category, seller_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, description, price, sku, inventory_count, image_path, category, seller_id),
        )


def fetch_products(
    *,
    search: Optional[str] = None,
    stock_filter: Optional[str] = None,
    sort: str = "newest",
    category: Optional[str] = None,
    seller_id: Optional[int] = None,
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

    if category and category.lower() != "all":
        conditions.append("LOWER(category) = ?")
        params.append(category.lower())

    if seller_id is not None:
        conditions.append("products.seller_id = ?")
        params.append(seller_id)

    order_by_map = {
        "newest": "products.id DESC",
        "oldest": "products.id ASC",
        "price_low": "price ASC, products.id DESC",
        "price_high": "price DESC, products.id DESC",
        "inventory_low": "inventory_count ASC, products.id DESC",
        "inventory_high": "inventory_count DESC, products.id DESC",
        "name_az": "LOWER(name) ASC, products.id DESC",
        "name_za": "LOWER(name) DESC, products.id DESC",
        "rating_high": "avg_rating DESC, review_count DESC, products.id DESC",
        "rating_low": "avg_rating ASC, review_count DESC, products.id DESC",
    }
    order_clause = order_by_map.get(sort, order_by_map["newest"])

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
                products.id,
                products.name,
                products.description,
                products.price,
                products.sku,
                products.inventory_count,
                products.image_path,
                products.category,
                products.seller_id,
                sellers.store_name AS seller_name,
                sellers.user_id AS seller_user_id,
                COALESCE(r.avg_rating, 0) AS avg_rating,
                COALESCE(r.review_count, 0) AS review_count
            FROM products
            LEFT JOIN sellers ON sellers.id = products.seller_id
            LEFT JOIN (
                SELECT product_id, AVG(rating) AS avg_rating, COUNT(*) AS review_count
                FROM product_reviews
                GROUP BY product_id
            ) AS r
            ON r.product_id = products.id
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
            SELECT
                products.id,
                products.name,
                products.description,
                products.price,
                products.sku,
                products.inventory_count,
                products.image_path,
                products.category,
                products.seller_id,
                sellers.store_name AS seller_name,
                sellers.user_id AS seller_user_id,
                COALESCE(r.avg_rating, 0) AS avg_rating,
                COALESCE(r.review_count, 0) AS review_count
            FROM products
            LEFT JOIN sellers ON sellers.id = products.seller_id
            LEFT JOIN (
                SELECT product_id, AVG(rating) AS avg_rating, COUNT(*) AS review_count
                FROM product_reviews
                GROUP BY product_id
            ) AS r
            ON r.product_id = products.id
            WHERE products.id IN ({placeholders})
            """,
            ordered_ids,
        )
        lookup = {int(row["id"]): dict(row) for row in rows}

    return [lookup[pid] for pid in ordered_ids if pid in lookup]


def create_user(username: str, password_hash: str, *, is_admin: bool = False, is_seller: bool = False) -> int:
    """Insert a new application user and return the id."""
    with get_connection() as conn:
        return _insert_and_return_id(
            conn,
            """
            INSERT INTO users (username, password_hash, is_admin, is_seller)
            VALUES (?, ?, ?, ?)
            """,
            (username, password_hash, int(is_admin), int(is_seller)),
        )


def get_user_by_username(username: str) -> Optional[Mapping[str, object]]:
    """Fetch a user record given a username."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, username, password_hash, created_at, is_admin, is_seller
            FROM users
            WHERE username = ?
            """,
            (username,),
        ).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: int) -> Optional[Mapping[str, object]]:
    """Fetch a user record by primary key."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, username, password_hash, created_at, is_admin, is_seller
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


def fetch_user_cart(user_id: int) -> Dict[int, int]:
    """Return the persisted cart quantities for the given user."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT product_id, quantity
            FROM user_cart_items
            WHERE user_id = ?
            """,
            (user_id,),
        )
        cart: Dict[int, int] = {}
        for row in rows:
            try:
                pid = int(row["product_id"])
                qty = int(row["quantity"])
            except (TypeError, ValueError):
                continue
            if qty > 0:
                cart[pid] = qty
    return cart


def replace_user_cart(user_id: int, cart: Mapping[int, int]) -> None:
    """Replace the persisted cart for a user with the provided mapping."""
    with get_connection() as conn:
        conn.execute("DELETE FROM user_cart_items WHERE user_id = ?", (user_id,))
        for product_id, quantity in cart.items():
            try:
                pid = int(product_id)
                qty = int(quantity)
            except (TypeError, ValueError):
                continue
            if qty <= 0:
                continue
            conn.execute(
                """
                INSERT INTO user_cart_items (user_id, product_id, quantity)
                VALUES (?, ?, ?)
                """,
                (user_id, pid, qty),
            )


def remove_user_cart_item(user_id: int, product_id: int) -> None:
    """Remove a single product from a user's persisted cart."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM user_cart_items WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        )


def clear_user_cart(user_id: int) -> None:
    """Delete all persisted cart items for the user."""
    with get_connection() as conn:
        conn.execute("DELETE FROM user_cart_items WHERE user_id = ?", (user_id,))


def fetch_users() -> Iterable[Mapping[str, object]]:
    """Return all application users ordered by newest first."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, username, created_at, is_admin, is_seller
            FROM users
            ORDER BY id DESC
            """
        )
        return [dict(row) for row in rows]


def create_seller_profile(
    user_id: int,
    store_name: str,
    *,
    description: Optional[str] = None,
    contact_email: Optional[str] = None,
) -> int:
    """Create a seller profile for the given user and flag them as a seller."""
    with get_connection() as conn:
        seller_id = _insert_and_return_id(
            conn,
            """
            INSERT INTO sellers (user_id, store_name, description, contact_email)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, store_name, description, contact_email),
        )
        conn.execute("UPDATE users SET is_seller = 1 WHERE id = ?", (user_id,))
        return seller_id


def get_seller_by_user_id(user_id: int) -> Optional[Mapping[str, object]]:
    """Return the seller profile for a given user id, if one exists."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, user_id, store_name, description, contact_email, created_at
            FROM sellers
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


def get_seller_by_id(seller_id: int) -> Optional[Mapping[str, object]]:
    """Return the seller metadata for the provided seller id."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, user_id, store_name, description, contact_email, created_at
            FROM sellers
            WHERE id = ?
            """,
            (seller_id,),
        ).fetchone()
    return dict(row) if row else None


def fetch_sellers() -> list[Mapping[str, object]]:
    """Return all sellers with associated usernames."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                sellers.id,
                sellers.store_name,
                sellers.description,
                sellers.contact_email,
                sellers.created_at,
                users.username
            FROM sellers
            JOIN users ON users.id = sellers.user_id
            ORDER BY sellers.id DESC
            """
        )
        return [dict(row) for row in rows]


def fetch_seller_products(seller_id: int) -> list[Mapping[str, object]]:
    """Return all catalogue entries owned by a specific seller."""
    return list(fetch_products(seller_id=seller_id))


def fetch_customers() -> list[Mapping[str, object]]:
    """Return customer records ordered by newest first."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, first_name, last_name, email, company, notes, created_at
            FROM customers
            ORDER BY id DESC
            """
        )
        return [dict(row) for row in rows]


def create_customer(
    first_name: str,
    last_name: str,
    email: str,
    *,
    company: Optional[str] = None,
    notes: Optional[str] = None,
) -> int:
    """Add a new customer record for downstream order tracking."""
    with get_connection() as conn:
        return _insert_and_return_id(
            conn,
            """
            INSERT INTO customers (first_name, last_name, email, company, notes)
            VALUES (?, ?, ?, ?, ?)
            """,
            (first_name, last_name, email, company, notes),
        )


def update_customer(
    customer_id: int,
    *,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    email: Optional[str] = None,
    company: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """Update an existing customer row."""
    fields: list[str] = []
    params: list[object] = []

    def _append(column: str, value: Optional[object]) -> None:
        if value is not None:
            fields.append(f"{column} = ?")
            params.append(value)

    _append("first_name", first_name)
    _append("last_name", last_name)
    _append("email", email)
    _append("company", company)
    _append("notes", notes)

    if not fields:
        return

    with get_connection() as conn:
        conn.execute(
            f"UPDATE customers SET {', '.join(fields)} WHERE id = ?",
            (*params, customer_id),
        )


def delete_customer(customer_id: int) -> None:
    """Remove a customer and cascade-delete dependent orders."""
    with get_connection() as conn:
        conn.execute("DELETE FROM customers WHERE id = ?", (customer_id,))


def get_customer(customer_id: int) -> Optional[Mapping[str, object]]:
    """Return a single customer record."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, first_name, last_name, email, company, notes, created_at
            FROM customers
            WHERE id = ?
            """,
            (customer_id,),
        ).fetchone()
    return dict(row) if row else None


def create_order(
    customer_id: int,
    *,
    seller_id: Optional[int] = None,
    status: str = "pending",
    total_amount: float = 0.0,
    notes: Optional[str] = None,
) -> int:
    """Insert an order snapshot for manual fulfilment tracking."""
    with get_connection() as conn:
        return _insert_and_return_id(
            conn,
            """
            INSERT INTO orders (customer_id, seller_id, status, total_amount, notes)
            VALUES (?, ?, ?, ?, ?)
            """,
            (customer_id, seller_id, status, total_amount, notes),
        )


def update_order(
    order_id: int,
    *,
    status: Optional[str] = None,
    total_amount: Optional[float] = None,
    notes: Optional[str] = None,
    seller_id: Optional[int] = None,
) -> None:
    """Update the provided order details."""
    fields: list[str] = []
    params: list[object] = []

    def _append(column: str, value: Optional[object]) -> None:
        if value is not None:
            fields.append(f"{column} = ?")
            params.append(value)

    _append("status", status)
    _append("total_amount", total_amount)
    _append("notes", notes)
    _append("seller_id", seller_id)

    if not fields:
        return

    fields.append("updated_at = CURRENT_TIMESTAMP")

    with get_connection() as conn:
        conn.execute(
            f"UPDATE orders SET {', '.join(fields)} WHERE id = ?",
            (*params, order_id),
        )


def delete_order(order_id: int) -> None:
    """Remove an order row."""
    with get_connection() as conn:
        conn.execute("DELETE FROM orders WHERE id = ?", (order_id,))


def fetch_orders() -> list[Mapping[str, object]]:
    """Return orders with denormalised customer/seller data."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                orders.id,
                orders.status,
                orders.total_amount,
                orders.notes,
                orders.created_at,
                orders.updated_at,
                customers.id AS customer_id,
                customers.first_name || ' ' || customers.last_name AS customer_name,
                customers.email AS customer_email,
                sellers.id AS seller_id,
                sellers.store_name AS seller_name
            FROM orders
            JOIN customers ON customers.id = orders.customer_id
            LEFT JOIN sellers ON sellers.id = orders.seller_id
            ORDER BY orders.id DESC
            """
        )
        return [dict(row) for row in rows]


def get_order(order_id: int) -> Optional[Mapping[str, object]]:
    """Fetch a single order with joined metadata."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                orders.id,
                orders.status,
                orders.total_amount,
                orders.notes,
                orders.created_at,
                orders.updated_at,
                orders.customer_id,
                orders.seller_id
            FROM orders
            WHERE orders.id = ?
            """,
            (order_id,),
        ).fetchone()
    return dict(row) if row else None


def delete_product(product_id: int) -> None:
    """Remove a product and any orphaned order items."""
    with get_connection() as conn:
        conn.execute("DELETE FROM products WHERE id = ?", (product_id,))


def seed_data() -> None:
    """Populate a modern electronics-focused catalogue so the UI has content."""
    samples = [
        {
            "name": "PulseLink HDMI 2.1 Cable 2m",
            "description": "Certified Ultra High Speed cable engineered for 4K120 and 8K displays with dynamic HDR.",
            "price": 22.0,
            "sku": "GL-CBL-HD21",
            "inventory": 72,
            "image_path": "img/products/pulselink-hdmi.svg",
            "category": "Connectivity",
        },
        {
            "name": "NanoMesh Dupont Jumper Set (120 pack)",
            "description": "Pre-crimped male and female Dupont jumpers in colour-coded harnesses for rapid prototyping.",
            "price": 14.5,
            "sku": "GL-JMP-120",
            "inventory": 180,
            "image_path": "img/products/nanomesh-jumpers.svg",
            "category": "Prototyping",
        },
        {
            "name": "VectorForge ATX Motherboard X790",
            "description": "Performance ATX board with DDR5 support, triple NVMe slots, and integrated Wi-Fi 7.",
            "price": 329.0,
            "sku": "GL-MBD-X790",
            "inventory": 26,
            "image_path": "img/products/vectorforge-motherboard.svg",
            "category": "Compute",
        },
        {
            "name": "QuantumBlade NVMe SSD 2TB",
            "description": "PCIe Gen4 M.2 solid state drive delivering 7,000 MB/s sequential reads with onboard heatsink.",
            "price": 199.0,
            "sku": "GL-SSD-QB2",
            "inventory": 64,
            "image_path": "img/products/quantumblade-ssd.svg",
            "category": "Storage",
        },
        {
            "name": "Helios 850W Modular PSU (80+ Platinum)",
            "description": "Fully modular power supply with low ripple output, digital monitoring, and silent mode.",
            "price": 189.0,
            "sku": "GL-PSU-850",
            "inventory": 41,
            "image_path": "img/products/helios-psu.svg",
            "category": "Power",
        },
        {
            "name": "AuroraFlex USB-C Hub Pro",
            "description": "Aluminium hub with Thunderbolt passthrough, dual 4K display outputs, and NVMe expansion bay.",
            "price": 129.0,
            "sku": "GL-HUB-AF9",
            "inventory": 58,
            "image_path": "img/products/auroraflex-hub.svg",
            "category": "Accessories",
        },
        {
            "name": "TitanEdge GPU Bracket (ARGB)",
            "description": "Adjustable support bracket with addressable lighting to eliminate GPU sag in tempered glass builds.",
            "price": 39.0,
            "sku": "GL-GPU-TED",
            "inventory": 97,
            "image_path": "img/products/titanedge-bracket.svg",
            "category": "Chassis",
        },
        {
            "name": "IonCore Thermal Paste X9",
            "description": "Nano-diamond thermal compound with low viscosity for high surface coverage and 12.5 W/mK conductivity.",
            "price": 11.0,
            "sku": "GL-THP-X9",
            "inventory": 240,
            "image_path": "img/products/ioncore-thermal.svg",
            "category": "Cooling",
        },
        {
            "name": "MatrixLab Precision Screwdriver Set",
            "description": "40-bit magnetic driver kit with knurled grip handle for electronics disassembly and repair.",
            "price": 54.0,
            "sku": "GL-TLS-M40",
            "inventory": 88,
            "image_path": "img/products/matrixlab-tools.svg",
            "category": "Tools",
        },
        {
            "name": "GridWave Wi-Fi 7 Router",
            "description": "Tri-band mesh-ready router with 10G WAN, OFDMA support, and quantum-resistant WPA4 firmware.",
            "price": 289.0,
            "sku": "GL-NET-GW7",
            "inventory": 35,
            "image_path": "img/products/gridwave-router.svg",
            "category": "Networking",
        },
        {
            "name": "LumenStrip Addressable LED Kit",
            "description": "5m addressable RGB LED strip with adhesive backing, USB-C controller, and open API.",
            "price": 59.0,
            "sku": "GL-LIT-LMK",
            "inventory": 120,
            "image_path": "img/products/lumenstrip-kit.svg",
            "category": "Lighting",
        },
        {
            "name": "OptiMesh 140mm PWM Fan (2 pack)",
            "description": "Low vibration 140mm fans with fluid dynamic bearings and daisy-chainable PWM headers.",
            "price": 44.0,
            "sku": "GL-FAN-140",
            "inventory": 105,
            "image_path": "img/products/optimesh-fan.svg",
            "category": "Cooling",
        },
        {
            "name": "CircuitNest Pico AI Dev Board",
            "description": "Edge-ready microcontroller with NPU co-processor, onboard sensors, and MicroPython tooling.",
            "price": 89.0,
            "sku": "GL-DEV-PICO",
            "inventory": 140,
            "image_path": "img/products/circuitnest-ai.svg",
            "category": "Embedded",
        },
        {
            "name": "AtlasEdge Robotics Control Kit",
            "description": "Modular CAN-enabled robotics controller with quad motor drivers, IMU, and ROS 2 templates.",
            "price": 499.0,
            "sku": "GL-ROB-AEX",
            "inventory": 18,
            "image_path": "img/products/atlasedge-robotics.svg",
            "category": "Robotics",
        },
        {
            "name": "BioFlux Wearable Sensor Pod",
            "description": "Multi-sensor wearable node capturing ECG, SpO₂, and motion data with encrypted BLE sync.",
            "price": 179.0,
            "sku": "GL-WBL-BFX",
            "inventory": 52,
            "image_path": "img/products/bioflux-wearable.svg",
            "category": "Wearables",
        },
        {
            "name": "SymphonyIQ Studio Interface",
            "description": "USB-C audio interface with dual preamps, onboard DSP profiles, and balanced monitor outs.",
            "price": 259.0,
            "sku": "GL-AUD-SIQ",
            "inventory": 44,
            "image_path": "img/products/symphonyiq-interface.svg",
            "category": "Audio",
        },
        {
            "name": "VoltStack Portable Power Deck",
            "description": "Stackable 600Wh lithium pack with pure sine inverter and solar MPPT input.",
            "price": 649.0,
            "sku": "GL-PWR-VSD",
            "inventory": 22,
            "image_path": "img/products/voltstack-power.svg",
            "category": "Power",
        },
        {
            "name": "CarbonWeave 3D Filament Bundle",
            "description": "Tri-spool bundle of carbon-infused nylon, PETG, and PLA tuned for engineering prints.",
            "price": 96.0,
            "sku": "GL-3DP-CWB",
            "inventory": 75,
            "image_path": "img/products/carbonweave-filament.svg",
            "category": "Fabrication",
        },
        {
            "name": "AetherGrid Smart Home Relay Hub",
            "description": "Secure matter-compatible relay hub with AI routines and local voice assistant support.",
            "price": 189.0,
            "sku": "GL-IOT-AGR",
            "inventory": 68,
            "image_path": "img/products/aethergrid-relay.svg",
            "category": "IoT",
        },
        {
            "name": "AquaSense Hydroponic Sensor Array",
            "description": "Industrial IP65 sensor cluster for EC, pH, temp, and nutrient flow with LoRaWAN uplink.",
            "price": 349.0,
            "sku": "GL-AGR-AQS",
            "inventory": 33,
            "image_path": "img/products/aquasense-array.svg",
            "category": "Sensors",
        },
        {
            "name": "HelioDrone Scout Frame Kit",
            "description": "Lightweight carbon frame with foldable arms, gimbal mounting rails, and power distribution bus.",
            "price": 279.0,
            "sku": "GL-DRN-HDS",
            "inventory": 27,
            "image_path": "img/products/heliodrone-scout.svg",
            "category": "Drones",
        },
        {
            "name": "SkyPath Satellite IoT Modem",
            "description": "Global LEO satellite modem with eSIM fallback, supporting MQTT and secure OTA updates.",
            "price": 399.0,
            "sku": "GL-COM-SPT",
            "inventory": 31,
            "image_path": "img/products/skypath-modem.svg",
            "category": "Communications",
        },
        {
            "name": "NovaPulse Laser Engraver Module",
            "description": "Diode laser module with auto-focus, enclosure interlock, and parametric design presets.",
            "price": 349.0,
            "sku": "GL-FAB-NPL",
            "inventory": 29,
            "image_path": "img/products/novapulse-engraver.svg",
            "category": "Fabrication",
        },
        {
            "name": "PulseGuard Network Sentinel Appliance",
            "description": "1U gateway with AI intrusion detection, inline traffic shaping, and zero-touch provisioning.",
            "price": 589.0,
            "sku": "GL-SEC-PGD",
            "inventory": 24,
            "image_path": "img/products/pulsegard-sentinel.svg",
            "category": "Security",
        },
        {
            "name": "TrackSense UWB Locator Beacons",
            "description": "Six-pack of ultra-wideband anchors with PoE and centimeter-level indoor positioning.",
            "price": 499.0,
            "sku": "GL-IOT-TSB",
            "inventory": 38,
            "image_path": "img/products/tracksense-uwb.svg",
            "category": "Positioning",
        },
        {
            "name": "LumenWave Solar Lighting Kit",
            "description": "Off-grid solar lighting kit with swappable battery cores and adaptive dusk scheduling.",
            "price": 219.0,
            "sku": "GL-ENG-LWS",
            "inventory": 46,
            "image_path": "img/products/lumenwave-solar.svg",
            "category": "Energy",
        },
        {
            "name": "QuantumWeave Edge AI Accelerator",
            "description": "PCIe accelerator with 32 TOPS INT8 performance, ONNX runtime support, and thermals for fanless rigs.",
            "price": 629.0,
            "sku": "GL-AI-QWA",
            "inventory": 21,
            "image_path": "img/products/quantumweave-accelerator.svg",
            "category": "AI Compute",
        },
    ]

    with get_connection() as conn:
        current_version = conn.execute("PRAGMA user_version").fetchone()[0]
        target_version = 6

        needs_refresh = current_version < target_version
        if not needs_refresh:
            product_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            needs_refresh = product_count == 0

        if needs_refresh:
            conn.execute("DELETE FROM products")
            for product in samples:
                conn.execute(
                    """
                    INSERT INTO products (name, description, price, sku, inventory_count, image_path, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        product["name"],
                        product["description"],
                        product["price"],
                        product["sku"],
                        product["inventory"],
                        product["image_path"],
                        product["category"],
                    ),
                )
            conn.execute(f"PRAGMA user_version = {target_version}")

        # Ensure sample users exist so seeded reviews can reference them.
        seed_password_hash = "scrypt:32768:8:1$3AM6jKXzTPSQGvK8$0d815eb8dc822a7e62bf03a95ef480cc214f736c3fa6b4080696c33f17893be63e24e7aaa975c98b4cf03fd51de4a17dfb884d3ef63992914099699db7b01512"
        seed_users = [
            ("gearloom_lab", seed_password_hash, 0, 1),
            ("field_ops", seed_password_hash, 0, 0),
            ("grid_support", seed_password_hash, 0, 0),
            ("circuit_artist", seed_password_hash, 0, 0),
            ("render_stack", seed_password_hash, 0, 0),
            ("ops_admin", seed_password_hash, 1, 0),
        ]
        for username, password_hash, is_admin, is_seller in seed_users:
            conn.execute(
                """
                INSERT INTO users (username, password_hash, is_admin, is_seller)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    password_hash = excluded.password_hash,
                    is_admin = excluded.is_admin,
                    is_seller = excluded.is_seller
                """,
                (username, password_hash, is_admin, is_seller),
            )

        # Ensure at least one seller profile exists for demo data ownership.
        seller_count = conn.execute("SELECT COUNT(*) FROM sellers").fetchone()[0]
        if seller_count == 0:
            seller_seeds = [
                {
                    "username": "gearloom_lab",
                    "store_name": "Gearloom Labs",
                    "description": "Lab team curating the launch catalogue.",
                    "contact_email": "labs@gearloom.io",
                }
            ]
            for seed in seller_seeds:
                user_row = conn.execute(
                    "SELECT id FROM users WHERE username = ?", (seed["username"],)
                ).fetchone()
                if not user_row:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO sellers (user_id, store_name, description, contact_email)
                    VALUES (?, ?, ?, ?)
                    """,
                    (user_row["id"], seed["store_name"], seed["description"], seed["contact_email"]),
                )
                conn.execute("UPDATE users SET is_seller = 1 WHERE id = ?", (user_row["id"],))
        seller_lookup = {
            row["username"]: row["seller_id"]
            for row in conn.execute(
                """
                SELECT users.username AS username, sellers.id AS seller_id
                FROM sellers
                JOIN users ON users.id = sellers.user_id
                """
            )
        }
        default_seller = conn.execute(
            "SELECT id FROM sellers ORDER BY id LIMIT 1"
        ).fetchone()
        if default_seller:
            conn.execute(
                "UPDATE products SET seller_id = ? WHERE seller_id IS NULL",
                (default_seller["id"],),
            )

        # Seed a couple of customers so analytics look alive.
        customer_count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        if customer_count == 0:
            conn.executemany(
                """
                INSERT INTO customers (first_name, last_name, email, company, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    ("Jamie", "Rivera", "jamie@example.com", "Axion Labs", "Prefers solar inventory."),
                    ("Taylor", "Bennett", "taylor@example.com", "FieldGrid", "Needs rush shipping."),
                    ("Morgan", "Vale", "morgan@example.com", "Lumen Dynamics", None),
                ],
            )

        order_count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        if order_count == 0:
            customer_lookup = {
                row["email"]: row["id"] for row in conn.execute("SELECT id, email FROM customers")
            }
            sample_orders = [
                ("jamie@example.com", "processing", 512.0, "AI controller kit build", "gearloom_lab"),
                ("taylor@example.com", "pending", 189.0, "Portable solar stack preorder", "gearloom_lab"),
                ("morgan@example.com", "fulfilled", 329.0, "Motherboard restock", "gearloom_lab"),
            ]
            for email, status, total_amount, notes, seller_username in sample_orders:
                customer_id = customer_lookup.get(email)
                seller_id = seller_lookup.get(seller_username)
                if not customer_id:
                    continue
                conn.execute(
                    """
                    INSERT INTO orders (customer_id, seller_id, status, total_amount, notes)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (customer_id, seller_id, status, total_amount, notes),
                )

        review_total = conn.execute("SELECT COUNT(*) FROM product_reviews").fetchone()[0]
        if review_total == 0:
            sku_lookup = {
                row["sku"]: row["id"]
                for row in conn.execute("SELECT id, sku FROM products WHERE sku IS NOT NULL")
            }
            user_names = [user for user, *_ in seed_users]
            placeholders = ", ".join("?" for _ in user_names)
            user_lookup = {}
            if user_names:
                user_lookup = {
                    row["username"]: row["id"]
                    for row in conn.execute(
                        f"SELECT id, username FROM users WHERE username IN ({placeholders})",
                        tuple(user_names),
                    )
                }
            sample_reviews = [
                ("GL-ROB-AEX", "gearloom_lab", 5, "Handled four actuators and a LiDAR rig without finicky tuning. Firmware hooks were spot on."),
                ("GL-DRN-HDS", "field_ops", 4, "Frame folds down fast and survived a windy survey. Would love a slightly wider power rail."),
                ("GL-PWR-VSD", "grid_support", 5, "Ran a field lab for two days with constant 600W draw. The MPPT kept solar intake steady."),
                ("GL-DEV-PICO", "circuit_artist", 4, "MicroPython examples flashed cleanly. Onboard IMU calibration doc was helpful."),
                ("GL-AI-QWA", "render_stack", 5, "Slotted into a compact inference node and chews through ONNX graphs without throttling."),
            ]
            for sku, username, rating, comment in sample_reviews:
                product_id = sku_lookup.get(sku)
                user_id = user_lookup.get(username)
                if not product_id or not user_id:
                    continue
                conn.execute(
                    """
                    INSERT INTO product_reviews (product_id, user_id, rating, comment)
                    VALUES (?, ?, ?, ?)
                    """,
                    (product_id, user_id, rating, comment),
                )


def get_product(product_id: int) -> Optional[Mapping[str, object]]:
    """Return a single product or None when not found."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                products.id,
                products.name,
                products.description,
                products.price,
                products.sku,
                products.inventory_count,
                products.image_path,
                products.category,
                products.seller_id,
                sellers.store_name AS seller_name,
                sellers.user_id AS seller_user_id,
                COALESCE(r.avg_rating, 0) AS avg_rating,
                COALESCE(r.review_count, 0) AS review_count
            FROM products
            LEFT JOIN sellers ON sellers.id = products.seller_id
            LEFT JOIN (
                SELECT product_id, AVG(rating) AS avg_rating, COUNT(*) AS review_count
                FROM product_reviews
                GROUP BY product_id
            ) AS r
            ON r.product_id = products.id
            WHERE products.id = ?
            """,
            (product_id,),
        ).fetchone()
        return dict(row) if row else None


def upsert_product_review(product_id: int, user_id: int, rating: int, comment: str | None = None) -> None:
    """Create or update a review for a product from a user."""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO product_reviews (product_id, user_id, rating, comment, created_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(product_id, user_id)
            DO UPDATE SET
                rating = excluded.rating,
                comment = excluded.comment,
                created_at = CURRENT_TIMESTAMP
            """,
            (product_id, user_id, rating, comment.strip() if comment else None),
        )


def fetch_product_reviews(product_id: int) -> Iterable[Mapping[str, object]]:
    """Return reviews for the given product ordered by newest first."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                product_reviews.id,
                product_reviews.rating,
                product_reviews.comment,
                product_reviews.created_at,
                product_reviews.user_id,
                users.username
            FROM product_reviews
            JOIN users ON users.id = product_reviews.user_id
            WHERE product_reviews.product_id = ?
            ORDER BY datetime(product_reviews.created_at) DESC
            """,
            (product_id,),
        )
        return [dict(row) for row in rows]


def get_user_review(product_id: int, user_id: int) -> Optional[Mapping[str, object]]:
    """Return a specific user's review for a product if present."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, rating, comment, created_at
            FROM product_reviews
            WHERE product_id = ? AND user_id = ?
            """,
            (product_id, user_id),
        ).fetchone()
        return dict(row) if row else None


def get_product_rating_summary(product_id: int) -> Mapping[str, float]:
    """Return the average rating and total review count for a product."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                COALESCE(AVG(rating), 0) AS avg_rating,
                COUNT(*) AS review_count
            FROM product_reviews
            WHERE product_id = ?
            """,
            (product_id,),
        ).fetchone()
        if not row:
            return {"avg_rating": 0.0, "review_count": 0}
        return {"avg_rating": float(row["avg_rating"]), "review_count": int(row["review_count"])}


def upsert_recent_product_view(user_id: int, product_id: int, max_items: int = 10) -> None:
    """Record that a user viewed a product, keeping only the latest entries."""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO user_recent_products (user_id, product_id, viewed_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id, product_id)
            DO UPDATE SET viewed_at = CURRENT_TIMESTAMP
            """,
            (user_id, product_id),
        )
        conn.execute(
            """
            DELETE FROM user_recent_products
            WHERE user_id = ? AND product_id NOT IN (
                SELECT product_id
                FROM user_recent_products
                WHERE user_id = ?
                ORDER BY datetime(viewed_at) DESC
                LIMIT ?
            )
            """,
            (user_id, user_id, max_items),
        )


def fetch_recent_products_for_user(user_id: int, limit: int = 5) -> list[Mapping[str, object]]:
    """Return the user's most recently viewed products."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                products.id,
                products.name,
                products.description,
                products.price,
                products.sku,
                products.inventory_count,
                products.image_path,
                products.category,
                products.seller_id,
                sellers.store_name AS seller_name,
                sellers.user_id AS seller_user_id,
                COALESCE(r.avg_rating, 0) AS avg_rating,
                COALESCE(r.review_count, 0) AS review_count,
                user_recent_products.viewed_at
            FROM user_recent_products
            JOIN products ON products.id = user_recent_products.product_id
            LEFT JOIN sellers ON sellers.id = products.seller_id
            LEFT JOIN (
                SELECT product_id, AVG(rating) AS avg_rating, COUNT(*) AS review_count
                FROM product_reviews
                GROUP BY product_id
            ) AS r ON r.product_id = products.id
            WHERE user_recent_products.user_id = ?
            ORDER BY datetime(user_recent_products.viewed_at) DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
        return [dict(row) for row in rows]


def fetch_product_categories() -> list[str]:
    """Return available product categories ordered alphabetically."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT category
            FROM products
            WHERE TRIM(category) <> ''
            ORDER BY LOWER(category)
            """
        )
        return [row["category"] for row in rows]


def update_product(
    product_id: int,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    price: Optional[float] = None,
    sku: Optional[str] = None,
    inventory_count: Optional[int] = None,
    image_path: Optional[str] = None,
    category: Optional[str] = None,
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
    _append("category", category)

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
    if "category" not in columns:
        conn.execute("ALTER TABLE products ADD COLUMN category TEXT NOT NULL DEFAULT 'General'")
    if "seller_id" not in columns:
        conn.execute("ALTER TABLE products ADD COLUMN seller_id INTEGER REFERENCES sellers(id)")


def _ensure_user_columns(conn: sqlite3.Connection) -> None:
    """Add role-related columns to the users table during upgrades."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(users)")}
    if "is_admin" not in columns:
        conn.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
    if "is_seller" not in columns:
        conn.execute("ALTER TABLE users ADD COLUMN is_seller INTEGER NOT NULL DEFAULT 0")
