"""SQLAlchemy-powered data layer for the Gearloom storefront."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, Mapping, Optional, Sequence

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    asc,
    desc,
    and_,
    create_engine,
    delete,
    func,
    or_,
    select,
    cast,
    update,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    aliased,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.sql import Select

from seed_data.product_catalog import PRODUCT_CATALOG
from security import decrypt_sensitive_value, encrypt_sensitive_value, hash_password

# --------------------------------------------------------------------------------------
# Small coercion helpers
# --------------------------------------------------------------------------------------


def _as_int(value: object, default: int = 0) -> int:
    """Best-effort conversion to int with a fallback."""

    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: object, default: float = 0.0) -> float:
    """Best-effort conversion to float with a fallback."""

    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------------------
# SQLAlchemy setup
# --------------------------------------------------------------------------------------

DB_PATH = Path(__file__).with_name("store.db")
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    future=True,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    is_seller: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    seller_profile: Mapped[Optional["Seller"]] = relationship("Seller", back_populates="user", uselist=False)
    cart_items: Mapped[list["UserCartItem"]] = relationship(
        "UserCartItem", back_populates="user", cascade="all, delete-orphan"
    )
    reviews: Mapped[list["ProductReview"]] = relationship(
        "ProductReview", back_populates="user", cascade="all, delete-orphan"
    )
    recent_views: Mapped[list["UserRecentProduct"]] = relationship(
        "UserRecentProduct", back_populates="user", cascade="all, delete-orphan"
    )
    orders: Mapped[list["Order"]] = relationship("Order", back_populates="user")


class Seller(Base):
    __tablename__ = "sellers"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), unique=True, nullable=False)
    store_name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    contact_email: Mapped[Optional[str]] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    user: Mapped[User] = relationship("User", back_populates="seller_profile")
    products: Mapped[list["Product"]] = relationship("Product", back_populates="seller")
    orders: Mapped[list["Order"]] = relationship("Order", back_populates="seller")


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    brand: Mapped[str] = mapped_column(String, nullable=False, default="", server_default="")
    description: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    sku: Mapped[Optional[str]] = mapped_column(String, unique=True)
    inventory_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    image_path: Mapped[Optional[str]] = mapped_column(String)
    category: Mapped[str] = mapped_column(String, nullable=False, default="General", server_default="General")
    seller_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sellers.id", ondelete="SET NULL"))

    seller: Mapped[Optional[Seller]] = relationship("Seller", back_populates="products")
    reviews: Mapped[list["ProductReview"]] = relationship(
        "ProductReview", back_populates="product", cascade="all, delete-orphan"
    )
    cart_items: Mapped[list["UserCartItem"]] = relationship(
        "UserCartItem", back_populates="product", cascade="all, delete-orphan"
    )
    recent_views: Mapped[list["UserRecentProduct"]] = relationship(
        "UserRecentProduct", back_populates="product", cascade="all, delete-orphan"
    )

class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    seller_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sellers.id", ondelete="SET NULL"))
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending", server_default="pending")
    total_amount: Mapped[float] = mapped_column(Float, nullable=False, default=0, server_default="0")
    notes: Mapped[Optional[str]] = mapped_column(Text)
    contact_name: Mapped[str] = mapped_column(String, nullable=False, default="", server_default="")
    contact_email: Mapped[str] = mapped_column(String, nullable=False, default="", server_default="")
    shipping_address: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    shipping_city: Mapped[str] = mapped_column(String, nullable=False, default="", server_default="")
    shipping_region: Mapped[Optional[str]] = mapped_column(String)
    shipping_postal: Mapped[str] = mapped_column(String, nullable=False, default="", server_default="")
    shipping_country: Mapped[str] = mapped_column(String, nullable=False, default="United States", server_default="United States")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp(), onupdate=func.current_timestamp()
    )

    user: Mapped[Optional[User]] = relationship("User", back_populates="orders", foreign_keys=[user_id])
    seller: Mapped[Optional[Seller]] = relationship("Seller", back_populates="orders")
    items: Mapped[list["OrderItem"]] = relationship(
        "OrderItem", back_populates="order", cascade="all, delete-orphan"
    )


class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), nullable=False)
    product_id: Mapped[Optional[int]] = mapped_column(ForeignKey("products.id", ondelete="SET NULL"))
    product_name: Mapped[str] = mapped_column(String, nullable=False)
    product_sku: Mapped[Optional[str]] = mapped_column(String)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    unit_price: Mapped[float] = mapped_column(Float, nullable=False, default=0, server_default="0")

    order: Mapped[Order] = relationship("Order", back_populates="items")


class UserCartItem(Base):
    __tablename__ = "user_cart_items"
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), primary_key=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")

    user: Mapped[User] = relationship("User", back_populates="cart_items")
    product: Mapped[Product] = relationship("Product", back_populates="cart_items")


class ProductReview(Base):
    __tablename__ = "product_reviews"
    __table_args__ = (UniqueConstraint("product_id", "user_id", name="uq_product_user_review"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    rating: Mapped[int] = mapped_column(Integer, nullable=False)
    comment: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    product: Mapped[Product] = relationship("Product", back_populates="reviews")
    user: Mapped[User] = relationship("User", back_populates="reviews")


class UserRecentProduct(Base):
    __tablename__ = "user_recent_products"
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), primary_key=True)
    viewed_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    user: Mapped[User] = relationship("User", back_populates="recent_views")
    product: Mapped[Product] = relationship("Product", back_populates="recent_views")


# --------------------------------------------------------------------------------------
# Session helper
# --------------------------------------------------------------------------------------


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""

    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# --------------------------------------------------------------------------------------
# Serialization helpers
# --------------------------------------------------------------------------------------


def _serialize_user(user: Optional[User]) -> Optional[dict[str, object]]:
    if not user:
        return None
    return {
        "id": user.id,
        "username": user.username,
        "password_hash": user.password_hash,
        "created_at": user.created_at,
        "is_admin": user.is_admin,
        "is_seller": user.is_seller,
    }


def _serialize_seller(seller: Optional[Seller]) -> Optional[dict[str, object]]:
    if not seller:
        return None
    return {
        "id": seller.id,
        "user_id": seller.user_id,
        "store_name": seller.store_name,
        "description": seller.description,
        "contact_email": seller.contact_email,
        "created_at": seller.created_at,
    }


# --------------------------------------------------------------------------------------
# Initialization and seeding
# --------------------------------------------------------------------------------------


def init_db() -> None:
    """Create tables and seed demo content."""

    Base.metadata.create_all(bind=engine)
    seed_data()


def seed_data() -> None:
    """Populate the catalogue with real products so the UI has content."""

    target_version = 10
    with engine.begin() as connection:
        current_version = connection.exec_driver_sql("PRAGMA user_version").scalar() or 0

    if current_version < target_version:
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP TABLE IF EXISTS order_items")
            connection.exec_driver_sql("DROP TABLE IF EXISTS orders")
            connection.exec_driver_sql("DROP TABLE IF EXISTS customers")
        Base.metadata.create_all(bind=engine)

    needs_refresh = False
    with session_scope() as session:
        product_count = session.scalar(select(func.count(Product.id))) or 0
        needs_refresh = current_version < target_version or product_count == 0

        if needs_refresh:
            session.execute(delete(ProductReview))
            session.execute(delete(OrderItem))
            session.execute(delete(Order))
            session.execute(delete(Product))
            for product in PRODUCT_CATALOG:
                session.add(
                    Product(
                        name=product["name"],
                        brand=product["brand"],
                        description=product["description"],
                        price=_as_float(product.get("price_aud")),
                        sku=product.get("sku"),
                        inventory_count=_as_int(product.get("inventory")),
                        image_path=product["image_path"],
                        category=product["category"],
                    )
                )

        seed_users = [
            ("gearloom_lab", "seed-pass", False, True),
            ("field_ops", "seed-pass", False, False),
            ("grid_support", "seed-pass", False, False),
            ("circuit_artist", "seed-pass", False, False),
            ("render_stack", "seed-pass", False, False),
            ("ops_admin", "seed-pass", True, False),
        ]
        for username, raw_password, is_admin, is_seller in seed_users:
            password_hash = hash_password(raw_password)
            user = session.execute(select(User).where(User.username == username)).scalar_one_or_none()
            if user:
                user.password_hash = password_hash
                user.is_admin = bool(is_admin)
                user.is_seller = bool(is_seller)
            else:
                session.add(
                    User(
                        username=username,
                        password_hash=password_hash,
                        is_admin=bool(is_admin),
                        is_seller=bool(is_seller),
                    )
                )

        seller_count = session.scalar(select(func.count(Seller.id))) or 0
        if seller_count == 0:
            demo_user = session.execute(select(User).where(User.username == "gearloom_lab")).scalar_one_or_none()
            if demo_user:
                session.add(
                    Seller(
                        user_id=demo_user.id,
                        store_name="Gearloom Labs",
                        description="Lab team curating the launch catalogue.",
                        contact_email="labs@gearloom.io",
                    )
                )
                demo_user.is_seller = True

        default_seller = session.execute(select(Seller).order_by(Seller.id)).scalars().first()
        if default_seller:
            session.execute(
                update(Product).where(Product.seller_id.is_(None)).values(seller_id=default_seller.id)
            )

        review_total = session.scalar(select(func.count(ProductReview.id))) or 0
        if review_total == 0:
            sku_lookup = {
                row.sku: row.id
                for row in session.execute(select(Product.id, Product.sku).where(Product.sku.is_not(None))).all()
            }
            user_lookup = {
                row.username: row.id for row in session.execute(select(User.id, User.username)).all()
            }
            sample_reviews = [
                (
                    "CPU-RYZEN7-7800X3D",
                    "gearloom_lab",
                    5,
                    "Paired it with a B650 board and the 3D V-Cache shaved latency right off our UE benchmark scene.",
                ),
                (
                    "SBC-RPI5-8GB",
                    "circuit_artist",
                    5,
                    "Finally a Pi with real PCIe access — the m.2 carrier plus dual 4K outputs runs our kiosk without hiccups.",
                ),
                (
                    "ROBO-INTEL-D455",
                    "field_ops",
                    4,
                    "Depth map stays stable outdoors, though we printed a sun visor to cut glare on noon surveys.",
                ),
                (
                    "TOOLS-HAKKO-FX888D",
                    "grid_support",
                    5,
                    "Heat recovery is instant and tips swap quickly when we're doing mixed lead-free repairs.",
                ),
                (
                    "DISP-DELL-U2723QE",
                    "render_stack",
                    4,
                    "IPS Black looks great and the built-in hub reduced cable clutter across the AI edit suite.",
                ),
            ]
            for sku, username, rating, comment in sample_reviews:
                product_id = sku_lookup.get(sku)
                user_id = user_lookup.get(username)
                if not product_id or not user_id:
                    continue
                session.add(
                    ProductReview(
                        product_id=product_id,
                        user_id=user_id,
                        rating=rating,
                        comment=comment,
                    )
                )

    if needs_refresh:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"PRAGMA user_version = {target_version}")


# --------------------------------------------------------------------------------------
# Product helpers
# --------------------------------------------------------------------------------------


def _product_select() -> tuple[Select, object, object]:
    """Return the base select for product queries with rating aggregates."""

    rating_summary = (
        select(
            ProductReview.product_id.label("product_id"),
            func.avg(ProductReview.rating).label("avg_rating"),
            func.count(ProductReview.id).label("review_count"),
        )
        .group_by(ProductReview.product_id)
        .subquery()
    )
    avg_rating_expr = func.coalesce(rating_summary.c.avg_rating, 0)
    review_count_expr = func.coalesce(rating_summary.c.review_count, 0)

    stmt = (
        select(
            Product.id,
            Product.name,
            Product.brand,
            Product.description,
            Product.price,
            Product.sku,
            Product.inventory_count,
            Product.image_path,
            Product.category,
            Product.seller_id,
            Seller.store_name.label("seller_name"),
            Seller.user_id.label("seller_user_id"),
            avg_rating_expr.label("avg_rating"),
            review_count_expr.label("review_count"),
        )
        .join(Seller, Product.seller, isouter=True)
        .join(rating_summary, rating_summary.c.product_id == Product.id, isouter=True)
    )
    return stmt, avg_rating_expr, review_count_expr


def insert_product(
    name: str,
    description: str,
    price: float,
    *,
    brand: str = "",
    sku: Optional[str] = None,
    inventory_count: int = 0,
    image_path: Optional[str] = None,
    category: str = "General",
    seller_id: Optional[int] = None,
) -> None:
    """Persist a new product using the ORM."""

    with session_scope() as session:
        session.add(
            Product(
                name=name,
                brand=brand,
                description=description,
                price=price,
                sku=sku,
                inventory_count=inventory_count,
                image_path=image_path,
                category=category,
                seller_id=seller_id,
            )
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

    stmt, avg_rating_expr, review_count_expr = _product_select()
    filters = []

    if search:
        like_term = f"%{search.lower()}%"
        filters.append(
            or_(
                func.lower(Product.name).like(like_term),
                func.lower(Product.brand).like(like_term),
                func.lower(Product.description).like(like_term),
                func.lower(func.coalesce(Product.sku, "")).like(like_term),
            )
        )

    stock_map = {
        "in": Product.inventory_count > 0,
        "low": and_(Product.inventory_count > 0, Product.inventory_count <= 10),
        "out": Product.inventory_count <= 0,
    }
    stock_clause = stock_map.get((stock_filter or "").lower())
    if stock_clause is not None:
        filters.append(stock_clause)

    if category and category.lower() != "all":
        filters.append(func.lower(Product.category) == category.lower())

    if seller_id is not None:
        filters.append(Product.seller_id == seller_id)

    if filters:
        stmt = stmt.where(and_(*filters))

    rating_expr = cast(avg_rating_expr, Float)
    review_expr = cast(review_count_expr, Integer)

    if sort == "rating_high":
        stmt = stmt.order_by(rating_expr.desc(), review_expr.desc(), Product.id.desc())
    elif sort == "rating_low":
        stmt = stmt.order_by(rating_expr.asc(), review_expr.desc(), Product.id.desc())
    else:
        order_map = {
            "newest": [Product.id.desc()],
            "oldest": [Product.id.asc()],
            "price_low": [Product.price.asc(), Product.id.desc()],
            "price_high": [Product.price.desc(), Product.id.desc()],
            "inventory_low": [Product.inventory_count.asc(), Product.id.desc()],
            "inventory_high": [Product.inventory_count.desc(), Product.id.desc()],
            "name_az": [func.lower(Product.name).asc(), Product.id.desc()],
            "name_za": [func.lower(Product.name).desc(), Product.id.desc()],
        }
        stmt = stmt.order_by(*order_map.get(sort, order_map["newest"]))

    with session_scope() as session:
        rows = session.execute(stmt).mappings().all()
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

    stmt, _, _ = _product_select()
    stmt = stmt.where(Product.id.in_(ordered_ids))

    with session_scope() as session:
        rows = session.execute(stmt).mappings().all()
        lookup = {int(row["id"]): dict(row) for row in rows}
    return [lookup[pid] for pid in ordered_ids if pid in lookup]


def create_user(username: str, password_hash: str, *, is_admin: bool = False, is_seller: bool = False) -> int:
    """Insert a new application user and return the id."""

    with session_scope() as session:
        user = User(
            username=username,
            password_hash=password_hash,
            is_admin=is_admin,
            is_seller=is_seller,
        )
        session.add(user)
        session.flush()
        return int(user.id)


def get_user_by_username(username: str) -> Optional[Mapping[str, object]]:
    """Fetch a user record given a username."""

    with session_scope() as session:
        user = session.execute(select(User).where(User.username == username)).scalar_one_or_none()
        return _serialize_user(user)


def delete_user(user_id: int) -> None:
    """Delete a user and cascade related data."""

    with session_scope() as session:
        session.execute(delete(User).where(User.id == user_id))


def get_user_by_id(user_id: int) -> Optional[Mapping[str, object]]:
    """Fetch a user by id."""

    with session_scope() as session:
        user = session.get(User, user_id)
        return _serialize_user(user)


def fetch_user_cart(user_id: int) -> Dict[int, int]:
    """Return the user's persisted cart items."""

    with session_scope() as session:
        rows = session.execute(
            select(UserCartItem.product_id, UserCartItem.quantity).where(UserCartItem.user_id == user_id)
        ).all()
        return {int(product_id): int(quantity) for product_id, quantity in rows}


def replace_user_cart(user_id: int, cart: Mapping[int, int]) -> None:
    """Replace all items in a user's cart with the provided mapping."""

    with session_scope() as session:
        session.execute(delete(UserCartItem).where(UserCartItem.user_id == user_id))
        for product_id, quantity in cart.items():
            try:
                pid = int(product_id)
                qty = max(1, int(quantity))
            except (TypeError, ValueError):
                continue
            session.add(UserCartItem(user_id=user_id, product_id=pid, quantity=qty))


def remove_user_cart_item(user_id: int, product_id: int) -> None:
    """Remove a single product from a user's persisted cart."""

    with session_scope() as session:
        session.execute(
            delete(UserCartItem).where(
                UserCartItem.user_id == user_id,
                UserCartItem.product_id == product_id,
            )
        )


def clear_user_cart(user_id: int) -> None:
    """Delete all persisted cart items for the user."""

    with session_scope() as session:
        session.execute(delete(UserCartItem).where(UserCartItem.user_id == user_id))


def fetch_users() -> Iterable[Mapping[str, object]]:
    """Return all application users ordered by newest first."""

    with session_scope() as session:
        users = session.execute(select(User).order_by(User.id.desc())).scalars().all()
        serialized = (_serialize_user(user) for user in users)
        return [user for user in serialized if user]


def create_seller_profile(
    user_id: int,
    store_name: str,
    *,
    description: Optional[str] = None,
    contact_email: Optional[str] = None,
) -> int:
    """Create a seller profile for the given user and flag them as a seller."""

    with session_scope() as session:
        seller = Seller(
            user_id=user_id,
            store_name=store_name,
            description=description,
            contact_email=contact_email,
        )
        session.add(seller)
        user = session.get(User, user_id)
        if user:
            user.is_seller = True
        session.flush()
        return int(seller.id)


def get_seller_by_user_id(user_id: int) -> Optional[Mapping[str, object]]:
    """Return the seller profile for a given user id, if one exists."""

    with session_scope() as session:
        seller = session.execute(select(Seller).where(Seller.user_id == user_id)).scalar_one_or_none()
        return _serialize_seller(seller)


def get_seller_by_id(seller_id: int) -> Optional[Mapping[str, object]]:
    """Return the seller metadata for the provided seller id."""

    with session_scope() as session:
        seller = session.get(Seller, seller_id)
        return _serialize_seller(seller)


def fetch_sellers() -> list[Mapping[str, object]]:
    """Return all sellers with associated usernames."""

    stmt = (
        select(
            Seller.id,
            Seller.user_id,
            Seller.store_name,
            Seller.description,
            Seller.contact_email,
            Seller.created_at,
            User.username,
        )
        .join(User, Seller.user)
        .order_by(Seller.id.desc())
    )
    with session_scope() as session:
        rows = session.execute(stmt).mappings().all()
        return [dict(row) for row in rows]


def fetch_seller_products(seller_id: int) -> list[Mapping[str, object]]:
    """Return all catalogue entries owned by a specific seller."""

    return list(fetch_products(seller_id=seller_id))


def create_order(
    *,
    user_id: Optional[int] = None,
    seller_id: Optional[int] = None,
    status: str = "pending",
    total_amount: float = 0.0,
    notes: Optional[str] = None,
    contact_name: str = "",
    contact_email: str = "",
    shipping_address: str = "",
    shipping_city: str = "",
    shipping_region: Optional[str] = None,
    shipping_postal: str = "",
    shipping_country: str = "United States",
    items: Optional[Iterable[Mapping[str, object]]] = None,
) -> int:
    """Insert an order snapshot for manual fulfilment tracking."""

    with session_scope() as session:
        order = Order(
            user_id=user_id,
            seller_id=seller_id,
            status=status,
            total_amount=total_amount,
            notes=notes,
            contact_name=encrypt_sensitive_value(contact_name or ""),
            contact_email=encrypt_sensitive_value(contact_email or ""),
            shipping_address=encrypt_sensitive_value(shipping_address or ""),
            shipping_city=encrypt_sensitive_value(shipping_city or ""),
            shipping_region=encrypt_sensitive_value(shipping_region) if shipping_region else None,
            shipping_postal=encrypt_sensitive_value(shipping_postal or ""),
            shipping_country=encrypt_sensitive_value(shipping_country or ""),
        )
        session.add(order)
        session.flush()
        if items:
            for item in items:
                product_name = str(item.get("product_name") or "").strip()
                if not product_name:
                    continue
                quantity = _as_int(item.get("quantity", 0))
                if quantity <= 0:
                    continue
                unit_price = _as_float(item.get("unit_price", 0.0))
                session.add(
                    OrderItem(
                        order_id=int(order.id),
                        product_id=item.get("product_id"),
                        product_name=product_name,
                        product_sku=item.get("product_sku"),
                        quantity=quantity,
                        unit_price=unit_price,
                    )
                )
        return int(order.id)


def update_order(
    order_id: int,
    *,
    status: Optional[str] = None,
    total_amount: Optional[float] = None,
    notes: Optional[str] = None,
    seller_id: Optional[int] = None,
    user_id: Optional[int] = None,
    contact_name: Optional[str] = None,
    contact_email: Optional[str] = None,
    shipping_address: Optional[str] = None,
    shipping_city: Optional[str] = None,
    shipping_region: Optional[str] = None,
    shipping_postal: Optional[str] = None,
    shipping_country: Optional[str] = None,
) -> None:
    """Update the provided order details."""

    with session_scope() as session:
        order = session.get(Order, order_id)
        if not order:
            return
        if status is not None:
            order.status = status
        if total_amount is not None:
            order.total_amount = total_amount
        if notes is not None:
            order.notes = notes
        if seller_id is not None:
            order.seller_id = seller_id
        if user_id is not None:
            order.user_id = user_id
        if contact_name is not None:
            order.contact_name = encrypt_sensitive_value(contact_name)
        if contact_email is not None:
            order.contact_email = encrypt_sensitive_value(contact_email)
        if shipping_address is not None:
            order.shipping_address = encrypt_sensitive_value(shipping_address)
        if shipping_city is not None:
            order.shipping_city = encrypt_sensitive_value(shipping_city)
        if shipping_region is not None:
            order.shipping_region = encrypt_sensitive_value(shipping_region) if shipping_region else None
        if shipping_postal is not None:
            order.shipping_postal = encrypt_sensitive_value(shipping_postal)
        if shipping_country is not None:
            order.shipping_country = encrypt_sensitive_value(shipping_country)


def delete_order(order_id: int) -> None:
    """Remove an order row."""

    with session_scope() as session:
        session.execute(delete(Order).where(Order.id == order_id))


def _order_items_for_ids(session: Session, order_ids: Iterable[int]) -> dict[int, list[dict[str, object]]]:
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for raw_id in order_ids:
        try:
            oid = int(raw_id)
        except (TypeError, ValueError):
            continue
        if oid in seen:
            continue
        normalized_ids.append(oid)
        seen.add(oid)
    if not normalized_ids:
        return {}
    rows = (
        session.execute(
            select(
                OrderItem.order_id,
                OrderItem.product_id,
                OrderItem.product_name,
                OrderItem.product_sku,
                OrderItem.quantity,
                OrderItem.unit_price,
            )
            .where(OrderItem.order_id.in_(normalized_ids))
            .order_by(OrderItem.id.asc())
        )
        .mappings()
        .all()
    )
    lookup: dict[int, list[dict[str, object]]] = {}
    for row in rows:
        order_id = int(row["order_id"])
        lookup.setdefault(order_id, []).append(
            {
                "product_id": row["product_id"],
                "product_name": row["product_name"],
                "product_sku": row["product_sku"],
                "quantity": int(row["quantity"]),
                "unit_price": float(row["unit_price"]),
            }
        )
    return lookup


def format_order_reference(order_id: int) -> str:
    """Return a human-friendly reference for an internal order id."""

    try:
        oid = int(order_id)
    except (TypeError, ValueError):
        oid = 0
    return f"Reservation GL-{oid:05d}"


def _hydrate_orders(rows: Sequence[Mapping[str, object]], session: Session) -> list[dict[str, object]]:
    """Attach decrypted contact details and line items to raw order rows."""

    if not rows:
        return []

    order_ids = [_as_int(row.get("id")) for row in rows if _as_int(row.get("id")) > 0]
    item_lookup = _order_items_for_ids(session, order_ids) if order_ids else {}
    hydrated: list[dict[str, object]] = []

    def _decrypt(payload: Mapping[str, object], key: str) -> str:
        return decrypt_sensitive_value(str(payload.get(key) or ""))

    for row in rows:
        payload = dict(row)
        oid = _as_int(payload.get("id"))
        if oid <= 0:
            continue
        items = item_lookup.get(oid, [])
        payload["items"] = items
        payload["item_count"] = sum(_as_int(item.get("quantity")) for item in items)
        payload["reference"] = format_order_reference(oid)
        payload["contact_name"] = _decrypt(payload, "contact_name")
        payload["contact_email"] = _decrypt(payload, "contact_email")
        payload["shipping_address"] = _decrypt(payload, "shipping_address")
        payload["shipping_city"] = _decrypt(payload, "shipping_city")
        payload["shipping_region"] = _decrypt(payload, "shipping_region") if payload.get("shipping_region") is not None else ""
        payload["shipping_postal"] = _decrypt(payload, "shipping_postal")
        payload["shipping_country"] = _decrypt(payload, "shipping_country")
        hydrated.append(payload)
    return hydrated


def fetch_orders() -> list[dict[str, object]]:
    """Return orders with denormalised customer/seller data."""

    order_user = aliased(User)
    stmt = (
        select(
            Order.id,
            Order.user_id,
            Order.status,
            Order.total_amount,
            Order.notes,
            Order.contact_name,
            Order.contact_email,
            Order.shipping_address,
            Order.shipping_city,
            Order.shipping_region,
            Order.shipping_postal,
            Order.shipping_country,
            Order.created_at,
            Order.updated_at,
            Seller.id.label("seller_id"),
            Seller.store_name.label("seller_name"),
            order_user.username.label("user_username"),
        )
        .join(Seller, Order.seller, isouter=True)
        .join(order_user, Order.user_id == order_user.id, isouter=True)
        .order_by(Order.id.desc())
    )
    with session_scope() as session:
        rows = [dict(row) for row in session.execute(stmt).mappings().all()]
        return _hydrate_orders(rows, session)


def fetch_orders_for_user(user_id: int) -> list[dict[str, object]]:
    """Return all orders tied to a specific user."""

    order_user = aliased(User)
    stmt = (
        select(
            Order.id,
            Order.user_id,
            Order.status,
            Order.total_amount,
            Order.notes,
            Order.contact_name,
            Order.contact_email,
            Order.shipping_address,
            Order.shipping_city,
            Order.shipping_region,
            Order.shipping_postal,
            Order.shipping_country,
            Order.created_at,
            Order.updated_at,
            Seller.id.label("seller_id"),
            Seller.store_name.label("seller_name"),
            order_user.username.label("user_username"),
        )
        .join(Seller, Order.seller, isouter=True)
        .join(order_user, Order.user_id == order_user.id, isouter=True)
        .where(Order.user_id == user_id)
        .order_by(Order.id.desc())
    )
    with session_scope() as session:
        rows = [dict(row) for row in session.execute(stmt).mappings().all()]
        return _hydrate_orders(rows, session)


def get_order(order_id: int) -> Optional[dict[str, object]]:
    """Fetch a single order with joined metadata."""

    stmt = select(
        Order.id,
        Order.user_id,
        Order.status,
        Order.total_amount,
        Order.notes,
        Order.contact_name,
        Order.contact_email,
        Order.shipping_address,
        Order.shipping_city,
        Order.shipping_region,
        Order.shipping_postal,
        Order.shipping_country,
        Order.created_at,
        Order.updated_at,
        Order.seller_id,
    ).where(Order.id == order_id)

    with session_scope() as session:
        rows = [dict(row) for row in session.execute(stmt).mappings().all()]
        hydrated = _hydrate_orders(rows, session)
        return hydrated[0] if hydrated else None


def delete_product(product_id: int) -> None:
    """Remove a product and any orphaned order items."""

    with session_scope() as session:
        session.execute(delete(Product).where(Product.id == product_id))


def get_product(product_id: int) -> Optional[Mapping[str, object]]:
    """Return a single product or None when not found."""

    stmt, _, _ = _product_select()
    stmt = stmt.where(Product.id == product_id)
    with session_scope() as session:
        row = session.execute(stmt).mappings().first()
        return dict(row) if row else None


def upsert_product_review(product_id: int, user_id: int, rating: int, comment: str | None = None) -> None:
    """Create or update a review for a product from a user."""

    with session_scope() as session:
        review = session.execute(
            select(ProductReview).where(
                ProductReview.product_id == product_id,
                ProductReview.user_id == user_id,
            )
        ).scalar_one_or_none()
        cleaned_comment = comment.strip() if comment else None
        if review:
            review.rating = rating
            review.comment = cleaned_comment
            review.created_at = datetime.utcnow()
        else:
            session.add(
                ProductReview(
                    product_id=product_id,
                    user_id=user_id,
                    rating=rating,
                    comment=cleaned_comment,
                )
            )


def fetch_product_reviews(product_id: int) -> Iterable[Mapping[str, object]]:
    """Return reviews for the given product ordered by newest first."""

    stmt = (
        select(
            ProductReview.id,
            ProductReview.rating,
            ProductReview.comment,
            ProductReview.created_at,
            ProductReview.user_id,
            User.username,
        )
        .join(User, ProductReview.user)
        .where(ProductReview.product_id == product_id)
        .order_by(ProductReview.created_at.desc())
    )
    with session_scope() as session:
        rows = session.execute(stmt).mappings().all()
        return [dict(row) for row in rows]


def get_user_review(product_id: int, user_id: int) -> Optional[Mapping[str, object]]:
    """Return a specific user's review for a product if present."""

    stmt = (
        select(
            ProductReview.id,
            ProductReview.rating,
            ProductReview.comment,
            ProductReview.created_at,
        )
        .where(ProductReview.product_id == product_id, ProductReview.user_id == user_id)
    )
    with session_scope() as session:
        row = session.execute(stmt).mappings().first()
        return dict(row) if row else None


def get_product_rating_summary(product_id: int) -> Mapping[str, float]:
    """Return the average rating and total review count for a product."""

    stmt = select(
        func.coalesce(func.avg(ProductReview.rating), 0).label("avg_rating"),
        func.count(ProductReview.id).label("review_count"),
    ).where(ProductReview.product_id == product_id)

    with session_scope() as session:
        row = session.execute(stmt).mappings().first()
    if not row:
        return {"avg_rating": 0.0, "review_count": 0}
    return {
        "avg_rating": float(row["avg_rating"]),
        "review_count": int(row["review_count"]),
    }


def upsert_recent_product_view(user_id: int, product_id: int, max_items: int = 10) -> None:
    """Record that a user viewed a product, keeping only the latest entries."""

    with session_scope() as session:
        entry = session.get(UserRecentProduct, (user_id, product_id))
        if entry:
            entry.viewed_at = datetime.utcnow()
        else:
            session.add(UserRecentProduct(user_id=user_id, product_id=product_id))

        recent_entries = (
            session.execute(
                select(UserRecentProduct)
                .where(UserRecentProduct.user_id == user_id)
                .order_by(UserRecentProduct.viewed_at.desc())
            )
            .scalars()
            .all()
        )
        for stale in recent_entries[max_items:]:
            session.delete(stale)


def fetch_recent_products_for_user(user_id: int, limit: int = 5) -> list[Mapping[str, object]]:
    """Return the user's most recently viewed products."""

    stmt, _, _ = _product_select()
    stmt = stmt.join(UserRecentProduct, UserRecentProduct.product_id == Product.id)
    stmt = stmt.add_columns(UserRecentProduct.viewed_at.label("viewed_at"))
    stmt = (
        stmt.where(UserRecentProduct.user_id == user_id)
        .order_by(UserRecentProduct.viewed_at.desc())
        .limit(limit)
    )

    with session_scope() as session:
        rows = session.execute(stmt).mappings().all()
        return [dict(row) for row in rows]


def fetch_product_categories() -> list[str]:
    """Return sorted unique product categories."""

    stmt = select(func.distinct(Product.category)).order_by(Product.category.asc())
    with session_scope() as session:
        return [row[0] for row in session.execute(stmt).all() if row[0]]


def update_product(
    product_id: int,
    *,
    name: Optional[str] = None,
    brand: Optional[str] = None,
    description: Optional[str] = None,
    price: Optional[float] = None,
    sku: Optional[str] = None,
    inventory_count: Optional[int] = None,
    image_path: Optional[str] = None,
    category: Optional[str] = None,
) -> None:
    """Update a product with provided fields."""

    with session_scope() as session:
        product = session.get(Product, product_id)
        if not product:
            return
        if name is not None:
            product.name = name
        if brand is not None:
            product.brand = brand
        if description is not None:
            product.description = description
        if price is not None:
            product.price = price
        if sku is not None:
            product.sku = sku
        if inventory_count is not None:
            product.inventory_count = inventory_count
        if image_path is not None:
            product.image_path = image_path
        if category is not None:
            product.category = category
