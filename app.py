"""Public-facing Flask application for the Chafe Escape storefront."""

from flask import Flask, render_template

from database import fetch_products, init_db

# Ensure the database and seed data exist before serving.
init_db()

app = Flask(__name__)


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


if __name__ == "__main__":
    app.run(debug=True)
