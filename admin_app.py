"""Admin-only Flask application for managing the store catalogue."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from uuid import uuid4

from flask import Flask, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from database import (
    delete_customer,
    delete_product,
    fetch_customers,
    fetch_products,
    get_product,
    init_db,
    insert_customer,
    insert_product,
    update_product,
)

# Ensure tables exist before the admin panel starts serving requests.
init_db()

admin_app = Flask(__name__)

# Keep uploads alongside the app so both Flask instances can serve them.
UPLOAD_DIR = Path(__file__).with_name("static").joinpath("uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}


@dataclass
class ProductFilterState:
    """Captured filter values plus derived helpers for rendering and redirects."""

    search: str = ""
    stock: str = "all"
    sort: str = "newest"
    fetch_kwargs: dict[str, object] = field(default_factory=dict)
    query_args: dict[str, str] = field(default_factory=dict)
    has_active: bool = False


def _empty_product_form() -> dict[str, str]:
    return {
        "name": "",
        "description": "",
        "price": "",
        "sku": "",
        "inventory_count": "",
    }


def _empty_customer_form() -> dict[str, str]:
    return {
        "first_name": "",
        "last_name": "",
        "email": "",
    }


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _save_image(file_storage) -> Optional[str]:
    """Persist an uploaded image and return the relative static path."""
    if not file_storage or not file_storage.filename:
        return None

    filename = secure_filename(file_storage.filename)
    if not _allowed_file(filename):
        raise ValueError("Please upload a PNG, JPG, GIF, or WEBP image.")

    extension = Path(filename).suffix
    unique_name = f"{uuid4().hex}{extension}"
    destination = UPLOAD_DIR / unique_name
    file_storage.save(destination)
    return f"uploads/{unique_name}"


def _delete_image(path: Optional[str]) -> None:
    if not path:
        return
    file_path = Path(__file__).with_name("static").joinpath(path)
    try:
        file_path.unlink()
    except FileNotFoundError:
        pass


def _form_text(field_name: str, fallback: str = "") -> str:
    """Return a trimmed version of a form field while avoiding attribute errors."""
    raw_value = request.form.get(field_name)
    if raw_value is None:
        return fallback
    return raw_value.strip()


def _parse_numeric_fields(price_raw: str, inventory_raw: str) -> tuple[Optional[float], Optional[int], Optional[str]]:
    """Convert price and inventory form inputs while sharing validation messaging."""
    try:
        price_value = float(price_raw)
        inventory_value = int(inventory_raw or 0)
    except ValueError:
        return None, None, "Price must be a number and inventory must be a whole number."
    return price_value, inventory_value, None


def _parse_product_filters() -> ProductFilterState:
    """Normalise product filter inputs used by the product editor."""
    search = request.args.get("search", "").strip()
    stock = request.args.get("stock", "all").lower()
    sort = request.args.get("sort", "newest").lower()

    allowed_stock = {"all", "in", "low", "out"}
    if stock not in allowed_stock:
        stock = "all"

    allowed_sort = {
        "newest",
        "oldest",
        "price_low",
        "price_high",
        "inventory_low",
        "inventory_high",
        "name_az",
        "name_za",
    }
    if sort not in allowed_sort:
        sort = "newest"

    fetch_kwargs: dict[str, object] = {"sort": sort}
    query_args: dict[str, str] = {}

    if search:
        fetch_kwargs["search"] = search
        query_args["search"] = search
    if stock != "all":
        fetch_kwargs["stock_filter"] = stock
        query_args["stock"] = stock
    if sort != "newest":
        query_args["sort"] = sort

    has_active = bool(search or stock != "all" or sort != "newest")

    return ProductFilterState(
        search=search,
        stock=stock,
        sort=sort,
        fetch_kwargs=fetch_kwargs,
        query_args=query_args,
        has_active=has_active,
    )


def _redirect_with_success(code: str, filters: ProductFilterState):
    """Return a redirect response while preserving any active filters."""
    params = {"success": code, **filters.query_args}
    return redirect(url_for("dashboard", **params))


@admin_app.route("/", methods=["GET", "POST"])
def dashboard():
    """Lightweight admin panel to manage products and customers."""
    message = None
    error = None

    filters = _parse_product_filters()
    product_form = _empty_product_form()
    customer_form = _empty_customer_form()

    if request.method == "POST":
        action = _form_text("action")

        if action == "create_product":
            product_form = {
                "name": _form_text("name"),
                "description": _form_text("description"),
                "price": _form_text("price"),
                "sku": _form_text("sku"),
                "inventory_count": _form_text("inventory_count"),
            }

            if not (product_form["name"] and product_form["description"] and product_form["price"]):
                error = "Name, description, and price are required."
            else:
                price, inventory_count, numeric_error = _parse_numeric_fields(
                    product_form["price"], product_form["inventory_count"]
                )
                if numeric_error:
                    error = numeric_error
                else:
                    try:
                        image = _save_image(request.files.get("image"))
                    except ValueError as exc:
                        error = str(exc)
                    else:
                        insert_product(
                            product_form["name"],
                            product_form["description"],
                            price if price is not None else 0.0,
                            sku=product_form["sku"] or None,
                            inventory_count=inventory_count if inventory_count is not None else 0,
                            image_path=image,
                        )
                        return _redirect_with_success("product_created", filters)

        elif action == "edit_product":
            try:
                product_id = int(request.form.get("product_id", ""))
            except ValueError:
                error = "Could not resolve the product to update."
            else:
                existing = get_product(product_id)
                if not existing:
                    error = "Could not find the product to update."
                else:
                    # Use the hidden field so admins can opt to clear the image entirely.
                    current_image = _form_text(
                        "current_image", existing.get("image_path") or ""
                    ) or existing.get("image_path")
                    should_remove_image = _form_text("remove_image") == "1"
                    name = _form_text("name", existing["name"])
                    description = _form_text("description", existing["description"])
                    price_raw = _form_text("price", f"{existing['price']}")
                    sku = _form_text("sku", existing["sku"] or "") or None
                    inventory_raw = _form_text(
                        "inventory_count", str(existing["inventory_count"])
                    )

                    price_value, inventory_value, numeric_error = _parse_numeric_fields(
                        price_raw, inventory_raw
                    )
                    if numeric_error:
                        error = numeric_error
                    else:
                        try:
                            new_image = _save_image(request.files.get("image"))
                        except ValueError as exc:
                            error = str(exc)
                        else:
                            image_path = current_image

                            if should_remove_image and current_image:
                                _delete_image(current_image)
                                image_path = None
                                current_image = None

                            if new_image:
                                if current_image and current_image != new_image:
                                    _delete_image(current_image)
                                image_path = new_image

                            update_product(
                                product_id,
                                name=name,
                                description=description,
                                price=price_value if price_value is not None else existing["price"],
                                sku=sku,
                                inventory_count=inventory_value if inventory_value is not None else existing["inventory_count"],
                                image_path=image_path,
                            )
                            return _redirect_with_success("product_updated", filters)

        elif action == "delete_product":
            try:
                product_id = int(request.form.get("product_id", ""))
            except ValueError:
                error = "Could not resolve the product to delete."
            else:
                product = get_product(product_id)
                if not product:
                    error = "Could not find the product to delete."
                else:
                    _delete_image(product.get("image_path"))
                    delete_product(product_id)
                    return _redirect_with_success("product_deleted", filters)

        elif action == "create_customer":
            customer_form = {
                "first_name": _form_text("first_name"),
                "last_name": _form_text("last_name"),
                "email": _form_text("email"),
            }

            if not all(customer_form.values()):
                error = "First name, last name, and email are required."
            else:
                insert_customer(
                    customer_form["first_name"],
                    customer_form["last_name"],
                    customer_form["email"],
                )
                return _redirect_with_success("customer_created", filters)

        elif action == "delete_customer":
            try:
                customer_id = int(request.form.get("customer_id", ""))
            except ValueError:
                error = "Could not resolve the customer to delete."
            else:
                delete_customer(customer_id)
                return _redirect_with_success("customer_deleted", filters)

        else:
            error = "Unknown admin action requested."

    success_messages = {
        "product_created": "Product saved.",
        "product_updated": "Product updated.",
        "product_deleted": "Product removed.",
        "customer_created": "Customer added.",
        "customer_deleted": "Customer removed.",
    }
    success_code = request.args.get("success")
    if success_code and success_code in success_messages:
        message = success_messages[success_code]

    products = list(fetch_products(**filters.fetch_kwargs))
    product_count = len(products)
    customers = fetch_customers()

    return render_template(
        "admin.html",
        message=message,
        error=error,
        product_form=product_form,
        customer_form=customer_form,
        products=products,
        customers=customers,
        product_filters=filters,
        product_count=product_count,
    )


if __name__ == "__main__":
    admin_app.run(debug=True, port=5001)
