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
                image_path TEXT
            );

            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS user_cart_items (
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (user_id, product_id),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
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


def create_user(username: str, password_hash: str) -> int:
    """Insert a new application user and return the id."""
    with get_connection() as conn:
        return _insert_and_return_id(
            conn,
            """
            INSERT INTO users (username, password_hash)
            VALUES (?, ?)
            """,
            (username, password_hash),
        )


def get_user_by_username(username: str) -> Optional[Mapping[str, object]]:
    """Fetch a user record given a username."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, username, password_hash, created_at
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
            SELECT id, username, password_hash, created_at
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
            SELECT id, username, created_at
            FROM users
            ORDER BY id DESC
            """
        )
        return [dict(row) for row in rows]


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
        },
        {
            "name": "NanoMesh Dupont Jumper Set (120 pack)",
            "description": "Pre-crimped male and female Dupont jumpers in colour-coded harnesses for rapid prototyping.",
            "price": 14.5,
            "sku": "GL-JMP-120",
            "inventory": 180,
            "image_path": "img/products/nanomesh-jumpers.svg",
        },
        {
            "name": "VectorForge ATX Motherboard X790",
            "description": "Performance ATX board with DDR5 support, triple NVMe slots, and integrated Wi-Fi 7.",
            "price": 329.0,
            "sku": "GL-MBD-X790",
            "inventory": 26,
            "image_path": "img/products/vectorforge-motherboard.svg",
        },
        {
            "name": "QuantumBlade NVMe SSD 2TB",
            "description": "PCIe Gen4 M.2 solid state drive delivering 7,000 MB/s sequential reads with onboard heatsink.",
            "price": 199.0,
            "sku": "GL-SSD-QB2",
            "inventory": 64,
            "image_path": "img/products/quantumblade-ssd.svg",
        },
        {
            "name": "Helios 850W Modular PSU (80+ Platinum)",
            "description": "Fully modular power supply with low ripple output, digital monitoring, and silent mode.",
            "price": 189.0,
            "sku": "GL-PSU-850",
            "inventory": 41,
            "image_path": "img/products/helios-psu.svg",
        },
        {
            "name": "AuroraFlex USB-C Hub Pro",
            "description": "Aluminium hub with Thunderbolt passthrough, dual 4K display outputs, and NVMe expansion bay.",
            "price": 129.0,
            "sku": "GL-HUB-AF9",
            "inventory": 58,
            "image_path": "img/products/auroraflex-hub.svg",
        },
        {
            "name": "TitanEdge GPU Bracket (ARGB)",
            "description": "Adjustable support bracket with addressable lighting to eliminate GPU sag in tempered glass builds.",
            "price": 39.0,
            "sku": "GL-GPU-TED",
            "inventory": 97,
            "image_path": "img/products/titanedge-bracket.svg",
        },
        {
            "name": "IonCore Thermal Paste X9",
            "description": "Nano-diamond thermal compound with low viscosity for high surface coverage and 12.5 W/mK conductivity.",
            "price": 11.0,
            "sku": "GL-THP-X9",
            "inventory": 240,
            "image_path": "img/products/ioncore-thermal.svg",
        },
        {
            "name": "MatrixLab Precision Screwdriver Set",
            "description": "40-bit magnetic driver kit with knurled grip handle for electronics disassembly and repair.",
            "price": 54.0,
            "sku": "GL-TLS-M40",
            "inventory": 88,
            "image_path": "img/products/matrixlab-tools.svg",
        },
        {
            "name": "GridWave Wi-Fi 7 Router",
            "description": "Tri-band mesh-ready router with 10G WAN, OFDMA support, and quantum-resistant WPA4 firmware.",
            "price": 289.0,
            "sku": "GL-NET-GW7",
            "inventory": 35,
            "image_path": "img/products/gridwave-router.svg",
        },
        {
            "name": "LumenStrip Addressable LED Kit",
            "description": "5m addressable RGB LED strip with adhesive backing, USB-C controller, and open API.",
            "price": 59.0,
            "sku": "GL-LIT-LMK",
            "inventory": 120,
            "image_path": "img/products/lumenstrip-kit.svg",
        },
        {
            "name": "OptiMesh 140mm PWM Fan (2 pack)",
            "description": "Low vibration 140mm fans with fluid dynamic bearings and daisy-chainable PWM headers.",
            "price": 44.0,
            "sku": "GL-FAN-140",
            "inventory": 105,
            "image_path": "img/products/optimesh-fan.svg",
        },
        {
            "name": "CircuitNest Pico AI Dev Board",
            "description": "Edge-ready microcontroller with NPU co-processor, onboard sensors, and MicroPython tooling.",
            "price": 89.0,
            "sku": "GL-DEV-PICO",
            "inventory": 140,
            "image_path": "img/products/circuitnest-ai.svg",
        },
        {
            "name": "AtlasEdge Robotics Control Kit",
            "description": "Modular CAN-enabled robotics controller with quad motor drivers, IMU, and ROS 2 templates.",
            "price": 499.0,
            "sku": "GL-ROB-AEX",
            "inventory": 18,
            "image_path": "img/products/atlasedge-robotics.svg",
        },
        {
            "name": "BioFlux Wearable Sensor Pod",
            "description": "Multi-sensor wearable node capturing ECG, SpO₂, and motion data with encrypted BLE sync.",
            "price": 179.0,
            "sku": "GL-WBL-BFX",
            "inventory": 52,
            "image_path": "img/products/bioflux-wearable.svg",
        },
        {
            "name": "SymphonyIQ Studio Interface",
            "description": "USB-C audio interface with dual preamps, onboard DSP profiles, and balanced monitor outs.",
            "price": 259.0,
            "sku": "GL-AUD-SIQ",
            "inventory": 44,
            "image_path": "img/products/symphonyiq-interface.svg",
        },
        {
            "name": "VoltStack Portable Power Deck",
            "description": "Stackable 600Wh lithium pack with pure sine inverter and solar MPPT input.",
            "price": 649.0,
            "sku": "GL-PWR-VSD",
            "inventory": 22,
            "image_path": "img/products/voltstack-power.svg",
        },
        {
            "name": "CarbonWeave 3D Filament Bundle",
            "description": "Tri-spool bundle of carbon-infused nylon, PETG, and PLA tuned for engineering prints.",
            "price": 96.0,
            "sku": "GL-3DP-CWB",
            "inventory": 75,
            "image_path": "img/products/carbonweave-filament.svg",
        },
        {
            "name": "AetherGrid Smart Home Relay Hub",
            "description": "Secure matter-compatible relay hub with AI routines and local voice assistant support.",
            "price": 189.0,
            "sku": "GL-IOT-AGR",
            "inventory": 68,
            "image_path": "img/products/aethergrid-relay.svg",
        },
        {
            "name": "AquaSense Hydroponic Sensor Array",
            "description": "Industrial IP65 sensor cluster for EC, pH, temp, and nutrient flow with LoRaWAN uplink.",
            "price": 349.0,
            "sku": "GL-AGR-AQS",
            "inventory": 33,
            "image_path": "img/products/aquasense-array.svg",
        },
        {
            "name": "HelioDrone Scout Frame Kit",
            "description": "Lightweight carbon frame with foldable arms, gimbal mounting rails, and power distribution bus.",
            "price": 279.0,
            "sku": "GL-DRN-HDS",
            "inventory": 27,
            "image_path": "img/products/heliodrone-scout.svg",
        },
        {
            "name": "SkyPath Satellite IoT Modem",
            "description": "Global LEO satellite modem with eSIM fallback, supporting MQTT and secure OTA updates.",
            "price": 399.0,
            "sku": "GL-COM-SPT",
            "inventory": 31,
            "image_path": "img/products/skypath-modem.svg",
        },
        {
            "name": "NovaPulse Laser Engraver Module",
            "description": "Diode laser module with auto-focus, enclosure interlock, and parametric design presets.",
            "price": 349.0,
            "sku": "GL-FAB-NPL",
            "inventory": 29,
            "image_path": "img/products/novapulse-engraver.svg",
        },
        {
            "name": "PulseGuard Network Sentinel Appliance",
            "description": "1U gateway with AI intrusion detection, inline traffic shaping, and zero-touch provisioning.",
            "price": 589.0,
            "sku": "GL-SEC-PGD",
            "inventory": 24,
            "image_path": "img/products/pulsegard-sentinel.svg",
        },
        {
            "name": "TrackSense UWB Locator Beacons",
            "description": "Six-pack of ultra-wideband anchors with PoE and centimeter-level indoor positioning.",
            "price": 499.0,
            "sku": "GL-IOT-TSB",
            "inventory": 38,
            "image_path": "img/products/tracksense-uwb.svg",
        },
        {
            "name": "LumenWave Solar Lighting Kit",
            "description": "Off-grid solar lighting kit with swappable battery cores and adaptive dusk scheduling.",
            "price": 219.0,
            "sku": "GL-ENG-LWS",
            "inventory": 46,
            "image_path": "img/products/lumenwave-solar.svg",
        },
        {
            "name": "QuantumWeave Edge AI Accelerator",
            "description": "PCIe accelerator with 32 TOPS INT8 performance, ONNX runtime support, and thermals for fanless rigs.",
            "price": 629.0,
            "sku": "GL-AI-QWA",
            "inventory": 21,
            "image_path": "img/products/quantumweave-accelerator.svg",
        },
    ]

    with get_connection() as conn:
        current_version = conn.execute("PRAGMA user_version").fetchone()[0]
        target_version = 5

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
