"""Public-facing Flask application for the Gearloom storefront."""

from __future__ import annotations

import math
import random
import re
from typing import Dict, List, Mapping, Union, cast

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.wrappers import Response

from database import (
    create_user,
    fetch_product_categories,
    fetch_product_reviews,
    fetch_products,
    fetch_products_by_ids,
    fetch_user_cart,
    get_product,
    get_product_rating_summary,
    get_user_by_id,
    get_user_by_username,
    get_user_review,
    init_db,
    replace_user_cart,
    upsert_product_review,
    upsert_recent_product_view,
    fetch_recent_products_for_user,
)

# Ensure the database and seed data exist before serving.
init_db()

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret-key-change-me"


def _current_user() -> Mapping[str, object] | None:
    """Return the authenticated user dict, clearing stale sessions if needed."""
    user_id = session.get("user_id")
    if user_id is None:
        return None
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        session.pop("user_id", None)
        session.pop("username", None)
        return None

    user = get_user_by_id(user_id_int)
    if not user:
        session.pop("user_id", None)
        session.pop("username", None)
        return None
    return user


def _current_user_id() -> int | None:
    """Return the authenticated user id if present."""
    user = _current_user()
    if not user:
        return None
    # user["id"] is typed as object; cast it to int for the type checker
    return int(cast(int, user["id"]))


def _get_cart() -> Dict[int, int]:
    """Return the active cart as an integer keyed mapping."""
    user_id = _current_user_id()
    if user_id:
        return fetch_user_cart(user_id)

    session.pop("cart", None)
    return {}


def _store_cart(cart: Dict[int, int]) -> None:
    """Persist the provided cart mapping."""
    user_id = _current_user_id()
    if user_id:
        replace_user_cart(user_id, cart)
        session["cart"] = {}
    else:
        session["cart"] = {}
    session.modified = True


@app.context_processor
def inject_cart_meta() -> Dict[str, object]:
    """Expose cart statistics and user details to every template."""
    cart = _get_cart()
    total_items = sum(cart.values())

    current_user = _current_user()

    return {"cart_item_count": total_items, "current_user": current_user}


@app.route("/")
def index() -> str:
    """Landing page with a snapshot of highlighted products."""
    products = list(fetch_products())
    sample_count = min(4, len(products))
    featured_products: List[Mapping[str, object]] = random.sample(products, sample_count) if sample_count else []

    left_count = math.ceil(sample_count / 2) if sample_count else 0
    left_products = featured_products[:left_count]
    right_products = featured_products[left_count:]

    return render_template(
        "index.html",
        featured_products=featured_products,
        left_products=left_products,
        right_products=right_products,
    )


@app.route("/about")
def about() -> str:
    """Static page with brand context."""
    return render_template("about.html")


@app.route("/help")
def help_center() -> str:
    """Customer support landing page."""
    return render_template("help.html")


@app.route("/returns")
def returns_policy() -> str:
    """Return and exchange policy."""
    return render_template("returns.html")


@app.route("/shipping")
def shipping_info() -> str:
    """Shipping and delivery details."""
    return render_template("shipping.html")


@app.route("/signup", methods=["GET", "POST"])
def signup() -> str | Response:
    """Register a new user account."""
    if _current_user_id():
        flash("You're already signed in.", "info")
        return redirect(url_for("index"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        password_confirm = request.form.get("password_confirm") or ""
        next_url = request.args.get("next") or request.form.get("next")

        if not username or not password:
            flash("Username and password are required.", "warning")
            return render_template("signup.html", username=username, next_url=next_url)
        if password != password_confirm:
            flash("Passwords do not match.", "warning")
            return render_template("signup.html", username=username, next_url=next_url)
        if get_user_by_username(username):
            flash("That username is already taken.", "warning")
            return render_template("signup.html", username=username, next_url=next_url)

        password_error = _validate_password(username, password)
        if password_error:
            flash(password_error, "warning")
            return render_template("signup.html", username=username, next_url=next_url)

        password_hash = generate_password_hash(password)
        new_user_id = create_user(username, password_hash)

        # Preserve any in-flight guest cart.
        guest_cart = _get_cart()

        session["user_id"] = new_user_id
        session["username"] = username
        _store_cart(guest_cart)

        flash("Welcome aboard! You're signed in.", "success")
        if next_url and next_url.startswith("/"):
            return redirect(next_url)
        return redirect(url_for("index"))

    next_url = request.args.get("next", "")
    return render_template("signup.html", next_url=next_url)


@app.route("/login", methods=["GET", "POST"])
def login() -> str | Response:
    """Authenticate an existing user."""
    if _current_user_id():
        flash("You're already signed in.", "info")
        return redirect(url_for("index"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        next_url = request.args.get("next") or request.form.get("next")

        user = get_user_by_username(username) if username else None
        if not user or not check_password_hash(cast(str, user["password_hash"]), password):
            flash("Invalid username or password.", "danger")
            return render_template("login.html", username=username, next_url=next_url)

        guest_cart = _get_cart()
        user_cart = fetch_user_cart(int(cast(int, user["id"])))

        merged_cart: Dict[int, int] = dict(user_cart)
        for product_id, qty in guest_cart.items():
            merged_cart[product_id] = merged_cart.get(product_id, 0) + qty

        session["user_id"] = int(cast(int, user["id"]))
        session["username"] = user["username"]
        _store_cart(merged_cart)

        flash("Signed in successfully.", "success")
        if next_url and next_url.startswith("/"):
            return redirect(next_url)
        return redirect(url_for("index"))

    next_url = request.args.get("next", "")
    return render_template("login.html", next_url=next_url)


@app.route("/logout")
def logout() -> Response:
    """Sign the user out and keep their cart as a guest snapshot."""
    session.pop("user_id", None)
    session.pop("username", None)
    session.pop("cart", None)
    session.modified = True
    flash("You have been signed out.", "info")
    return redirect(url_for("index"))


@app.route("/products")
def products() -> str:
    """Product catalogue with live inventory data."""
    search = (request.args.get("search") or "").strip()
    stock_raw = (request.args.get("stock") or "all").lower()
    sort_raw = (request.args.get("sort") or "newest").lower()
    category_raw = (request.args.get("category") or "all").strip()

    allowed_stock = {"all", "in", "low", "out"}
    stock = stock_raw if stock_raw in allowed_stock else "all"

    allowed_sort = {
        "newest",
        "oldest",
        "price_low",
        "price_high",
        "inventory_low",
        "inventory_high",
        "name_az",
        "name_za",
        "rating_high",
        "rating_low",
    }
    sort = sort_raw if sort_raw in allowed_sort else "newest"

    available_categories = fetch_product_categories()
    normalized_categories = {cat.lower(): cat for cat in available_categories}
    category_key = category_raw.lower()
    if category_key in normalized_categories:
        category = normalized_categories[category_key]
    elif category_key == "all":
        category = "all"
    else:
        category = "all"

    fetch_kwargs: dict[str, object] = {"sort": sort}
    if search:
        fetch_kwargs["search"] = search
    if stock != "all":
        fetch_kwargs["stock_filter"] = stock
    if category != "all":
        fetch_kwargs["category"] = category

    catalogue = list(fetch_products(**fetch_kwargs))
    user_id = _current_user_id()
    recently_viewed = fetch_recent_products_for_user(user_id, limit=5) if user_id else []
    filters = {
        "search": search,
        "stock": stock,
        "sort": sort,
        "category": category if category != "all" else "all",
        "has_active": bool(
            search or stock != "all" or sort != "newest" or category != "all"
        ),
        "result_count": len(catalogue),
    }

    category_options = ["All"] + available_categories

    return render_template(
        "products.html",
        products=catalogue,
        filters=filters,
        category_options=category_options,
        selected_category=category,
        recently_viewed=recently_viewed,
    )

@app.route("/products/<int:product_id>", methods=["GET", "POST"])
def product_detail(product_id: int) -> Union[str, Response]:
    """Detailed view of a single product with cart actions."""
    product = get_product(product_id)
    if not product:
        abort(404)

    user_id = _current_user_id()
    if user_id:
        upsert_recent_product_view(user_id, product_id)

    if request.method == "POST":
        form_type = request.form.get("form_type", "add_to_cart")
        if form_type == "review":
            if not user_id:
                flash("Sign in to leave a review.", "warning")
                return redirect(url_for("login", next=url_for("product_detail", product_id=product_id)))
            rating_raw = request.form.get("rating", "")
            comment = (request.form.get("comment") or "").strip()
            try:
                rating_value = int(rating_raw)
            except (TypeError, ValueError):
                flash("Select a rating between 1 and 5 stars.", "warning")
                return redirect(url_for("product_detail", product_id=product_id) + "#reviews")
            rating_value = max(1, min(5, rating_value))
            upsert_product_review(product_id, user_id, rating_value, comment)
            flash("Thanks for sharing your feedback!", "success")
            return redirect(url_for("product_detail", product_id=product_id) + "#reviews")

        if not user_id:
            flash("Sign in to add items to your cart.", "warning")
            login_target = url_for("login", next=url_for("product_detail", product_id=product_id))
            return redirect(login_target)

        quantity_raw = request.form.get("quantity", "1")
        try:
            quantity_value = max(1, int(quantity_raw))
        except (TypeError, ValueError):
            flash("Please choose a valid quantity.", "warning")
            return redirect(url_for("product_detail", product_id=product_id))

        cart = _get_cart()
        cart[product_id] = cart.get(product_id, 0) + quantity_value
        _store_cart(cart)
        flash(f"Added {quantity_value} × {product['name']} to your cart.", "success")
        return redirect(url_for("cart"))

    rating_summary = get_product_rating_summary(product_id)
    reviews = fetch_product_reviews(product_id)
    user_review = get_user_review(product_id, user_id) if user_id else None

    return render_template(
        "product_detail.html",
        product=product,
        rating_summary=rating_summary,
        reviews=reviews,
        user_review=user_review,
    )


@app.route("/cart")
def cart() -> str:
    """Display the current basket with totals."""
    cart = _get_cart()
    product_ids = list(cart.keys())
    items = []
    total = 0.0

    if product_ids:
        products = fetch_products_by_ids(product_ids)
        lookup = {product["id"]: product for product in products}
        for product_id in product_ids:
            product = lookup.get(product_id)
            if not product:
                continue
            quantity = cart[product_id]
            line_total = float(str(product["price"])) * quantity
            total += line_total
            items.append(
                {
                    "product": product,
                    "quantity": quantity,
                    "line_total": line_total,
                }
            )

    return render_template("cart.html", items=items, total=total)


@app.post("/cart/update/<int:product_id>")
def update_cart(product_id: int):
    """Adjust the quantity for a product already in the cart."""
    if not _current_user_id():
        flash("Sign in to manage your cart.", "warning")
        return redirect(url_for("login", next=url_for("cart")))
    cart = _get_cart()
    if product_id not in cart:
        flash("That product was not in your cart.", "warning")
        return redirect(url_for("cart"))

    quantity_raw = request.form.get("quantity", "1")
    try:
        quantity_value = int(quantity_raw)
    except (TypeError, ValueError):
        flash("Please enter a whole number for quantity.", "warning")
        return redirect(url_for("cart"))

    if quantity_value <= 0:
        cart.pop(product_id, None)
        flash("Removed the product from your cart.", "info")
    else:
        cart[product_id] = quantity_value
        flash("Updated your cart.", "success")

    _store_cart(cart)
    return redirect(url_for("cart"))


@app.post("/cart/remove/<int:product_id>")
def remove_from_cart(product_id: int):
    """Remove a product from the session cart."""
    if not _current_user_id():
        flash("Sign in to manage your cart.", "warning")
        return redirect(url_for("login", next=url_for("cart")))
    cart = _get_cart()
    if cart.pop(product_id, None) is not None:
        flash("Removed the product from your cart.", "info")
        _store_cart(cart)
    else:
        flash("That product was already removed.", "warning")
    return redirect(url_for("cart"))


@app.post("/cart/clear")
def clear_cart():
    """Empty the cart entirely."""
    if not _current_user_id():
        flash("Sign in to manage your cart.", "warning")
        return redirect(url_for("login", next=url_for("cart")))
    _store_cart({})
    flash("Cleared your cart.", "info")
    return redirect(url_for("cart"))


def _validate_password(username: str, password: str) -> str | None:
    """Return an error message if the password fails validation, otherwise None."""
    lowered = password.lower()
    username_lower = username.lower()

    if len(password) < 8:
        return "Password must be at least eight characters."
    if lowered in {"password", "password1", "letmein", "1234", "12345", "123456", "qwerty"}:
        return "Please choose a less common password."
    if lowered == username_lower:
        return "Password cannot match the username."
    if lowered in {f"{username_lower}{d}" for d in ("123", "1", "01")}:
        return "Password is too closely related to the username."
    if lowered.isdigit():
        return "Password must include letters in addition to numbers."
    if lowered.isalpha():
        return "Password must include at least one number or symbol."
    if re.match(r"(.)\1{2,}", lowered):
        return "Password cannot contain the same character repeated three or more times consecutively."

    return None


if __name__ == "__main__":
    app.run(debug=True)
