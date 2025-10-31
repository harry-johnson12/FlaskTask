"""Public-facing Flask application for the Chafe Escape storefront."""

from __future__ import annotations

from typing import Dict

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

from database import (
    fetch_products,
    fetch_products_by_ids,
    get_product,
    init_db,
)

# Ensure the database and seed data exist before serving.
init_db()

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret-key-change-me"


def _get_cart() -> Dict[int, int]:
    """Return the session cart as an integer keyed mapping."""
    raw_cart = session.get("cart", {})
    cart: Dict[int, int] = {}
    for product_id, quantity in raw_cart.items():
        try:
            pid = int(product_id)
            qty = int(quantity)
        except (TypeError, ValueError):
            continue
        if qty > 0:
            cart[pid] = qty
    return cart


def _store_cart(cart: Dict[int, int]) -> None:
    """Persist the provided cart mapping into the session."""
    session["cart"] = {str(pid): qty for pid, qty in cart.items() if qty > 0}
    session.modified = True


@app.context_processor
def inject_cart_meta() -> Dict[str, int]:
    """Expose cart statistics to every template."""
    cart = session.get("cart", {})
    total_items = 0
    for qty in cart.values():
        try:
            total_items += int(qty)
        except (TypeError, ValueError):
            continue
    return {"cart_item_count": total_items}


@app.route("/")
def index() -> str:
    """Landing page with a snapshot of highlighted products."""
    products = list(fetch_products())
    featured_products = products[:3]
    return render_template("index.html", featured_products=featured_products)


@app.route("/about")
def about() -> str:
    """Static page with brand context."""
    return render_template("about.html")


@app.route("/products")
def products() -> str:
    """Product catalogue with live inventory data."""
    products = fetch_products()
    return render_template("products.html", products=products)


@app.route("/products/<int:product_id>", methods=["GET", "POST"])
def product_detail(product_id: int) -> str:
    """Detailed view of a single product with cart actions."""
    product = get_product(product_id)
    if not product:
        abort(404)

    if request.method == "POST":
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

    return render_template("product_detail.html", product=product)


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
            line_total = float(product["price"]) * quantity
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
    session.pop("cart", None)
    flash("Cleared your cart.", "info")
    return redirect(url_for("cart"))


if __name__ == "__main__":
    app.run(debug=True)
