"""Admin-only Flask application for managing customers and orders."""

from __future__ import annotations

from functools import wraps
from typing import Optional, cast

from flask import Flask, flash, redirect, render_template, request, send_from_directory, session, url_for

from database import (
    create_user,
    delete_product,
    delete_user,
    fetch_orders,
    fetch_seller_products,
    fetch_sellers,
    fetch_users,
    get_user_by_id,
    get_user_by_username,
    init_db,
)
from security import hash_password, verify_password
from app import _validate_password  # type: ignore  # reuse password policy
from security import hash_password, verify_password

# Ensure tables exist before the admin panel starts serving requests.
init_db()

admin_app = Flask(__name__)
admin_app.config["SECRET_KEY"] = "dev-secret-key-change-me"
admin_app.config["SESSION_COOKIE_NAME"] = "gearloom-admin-session"


def _current_admin() -> Optional[dict[str, object]]:
    user_id = session.get("admin_user_id")
    if user_id is None:
        return None
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        session.pop("admin_user_id", None)
        return None
    user = get_user_by_id(user_id_int)
    if not user or not user.get("is_admin"):
        session.pop("admin_user_id", None)
        return None
    return dict(user)


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not _current_admin():
            flash("Sign in as an admin to access the console.", "warning")
            next_url = request.path if request.path else url_for("dashboard")
            return redirect(url_for("login", next=next_url))
        return view(*args, **kwargs)

    return wrapped


def _form_text(field_name: str, default: str = "") -> str:
    raw_value = request.form.get(field_name)
    if raw_value is None:
        return default
    return raw_value.strip()


@admin_app.context_processor
def inject_admin_user() -> dict[str, object]:
    return {"admin_user": _current_admin()}


@admin_app.route("/login", methods=["GET", "POST"])
def login():
    """Simple admin-only login tied to the shared user table."""
    if _current_admin():
        return redirect(url_for("dashboard"))

    next_url = request.args.get("next", url_for("dashboard"))
    if request.method == "POST":
        username = _form_text("username")
        password = request.form.get("password") or ""
        next_url = request.form.get("next") or next_url

        user = get_user_by_username(username) if username else None
        if not user or not user.get("is_admin"):
            flash("Invalid admin credentials.", "danger")
        elif not verify_password(password, cast(str, user["password_hash"])):
            flash("Invalid admin credentials.", "danger")
        else:
            session["admin_user_id"] = int(cast(int, user["id"]))
            flash("Welcome back.", "success")
            if next_url and next_url.startswith("/"):
                return redirect(next_url)
            return redirect(url_for("dashboard"))

    return render_template("admin_login.html", next_url=next_url)


@admin_app.route("/logout")
def logout():
    session.pop("admin_user_id", None)
    flash("Signed out of the admin console.", "info")
    return redirect(url_for("login"))


@admin_app.route("/service-worker.js")
def admin_service_worker():
    """Serve the shared PWA service worker for admin pages."""
    return send_from_directory(admin_app.static_folder, "service-worker.js", mimetype="application/javascript")


@admin_app.route("/", methods=["GET", "POST"])
@admin_required
def dashboard():
    """Admin console focused on customers, orders, and account visibility."""

    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "create_user_account":
            username = _form_text("username")
            password = request.form.get("password") or ""
            password_confirm = request.form.get("password_confirm") or ""

            if not username or not password:
                flash("Username and password are required.", "warning")
                return redirect(url_for("dashboard"))
            if password != password_confirm:
                flash("Passwords do not match.", "danger")
                return redirect(url_for("dashboard"))
            if get_user_by_username(username):
                flash("That username already exists.", "danger")
                return redirect(url_for("dashboard"))

            password_error = _validate_password(username, password)
            if password_error:
                flash(password_error, "warning")
                return redirect(url_for("dashboard"))

            password_hash = hash_password(password)
            create_user(username, password_hash)
            flash("User account created.", "success")
            return redirect(url_for("dashboard"))

        if action == "delete_user":
            try:
                target_user_id = int(request.form.get("user_id", ""))
            except (TypeError, ValueError):
                flash("Could not determine which user to delete.", "danger")
                return redirect(url_for("dashboard"))

            admin_user = _current_admin()
            if admin_user and int(admin_user["id"]) == target_user_id:
                flash("You cannot delete the currently signed-in admin.", "warning")
                return redirect(url_for("dashboard"))

            delete_user(target_user_id)
            flash("User removed.", "info")
            return redirect(url_for("dashboard"))

        if action == "delete_product":
            try:
                product_id = int(request.form.get("product_id", ""))
            except (TypeError, ValueError):
                flash("Could not resolve the product to delete.", "danger")
            else:
                delete_product(product_id)
                flash("Product removed.", "info")
            return redirect(url_for("dashboard"))

        flash("Unknown admin action.", "warning")
        return redirect(url_for("dashboard"))

    sellers = fetch_sellers()
    users = fetch_users()

    seller_lookup = {seller["user_id"]: seller for seller in sellers}
    seller_products = {
        int(seller["id"]): fetch_seller_products(int(seller["id"])) for seller in sellers
    }
    user_cards: list[dict[str, object]] = []
    for user in users:
        seller = seller_lookup.get(user["id"])
        products = seller_products.get(int(seller["id"])) if seller else []
        user_cards.append(
            {
                "user": user,
                "seller": seller,
                "products": products or [],
            }
        )

    orders = fetch_orders()

    return render_template(
        "admin.html",
        user_cards=user_cards,
        orders=orders,
    )


if __name__ == "__main__":
    admin_app.run(debug=True, port=5001)
