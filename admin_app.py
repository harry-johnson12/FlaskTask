"""Admin-only Flask application for managing customers and orders."""

from __future__ import annotations

from functools import wraps
from typing import Optional, cast

from flask import Flask, flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash

from database import (
    create_customer,
    create_order,
    delete_customer,
    delete_order,
    fetch_customers,
    fetch_orders,
    fetch_sellers,
    fetch_users,
    get_customer,
    get_order,
    get_user_by_id,
    get_user_by_username,
    init_db,
    update_customer,
    update_order,
)

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
        elif not check_password_hash(cast(str, user["password_hash"]), password):
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


@admin_app.route("/", methods=["GET", "POST"])
@admin_required
def dashboard():
    """Admin console focused on customers, orders, and account visibility."""
    status_choices = ["pending", "processing", "fulfilled", "cancelled"]

    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "create_customer":
            first_name = _form_text("first_name")
            last_name = _form_text("last_name")
            email = _form_text("email")
            company = _form_text("company") or None
            notes = _form_text("notes") or None
            if not (first_name and last_name and email):
                flash("First name, last name, and email are required.", "warning")
            else:
                create_customer(first_name, last_name, email, company=company, notes=notes)
                flash("Customer created.", "success")
            return redirect(url_for("dashboard"))

        if action == "update_customer":
            try:
                customer_id = int(request.form.get("customer_id", ""))
            except (TypeError, ValueError):
                flash("Could not resolve the customer to update.", "danger")
                return redirect(url_for("dashboard"))

            first_name = _form_text("first_name")
            last_name = _form_text("last_name")
            email = _form_text("email")
            if not (first_name and last_name and email):
                flash("First name, last name, and email are required.", "warning")
                return redirect(url_for("dashboard"))

            update_customer(
                customer_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                company=_form_text("company") or None,
                notes=_form_text("notes") or None,
            )
            flash("Customer updated.", "success")
            return redirect(url_for("dashboard"))

        if action == "delete_customer":
            try:
                customer_id = int(request.form.get("customer_id", ""))
            except (TypeError, ValueError):
                flash("Could not resolve the customer to delete.", "danger")
            else:
                delete_customer(customer_id)
                flash("Customer removed.", "info")
            return redirect(url_for("dashboard"))

        if action == "create_order":
            try:
                customer_id = int(request.form.get("customer_id", ""))
            except (TypeError, ValueError):
                flash("Select a customer for the order.", "warning")
                return redirect(url_for("dashboard"))

            if not get_customer(customer_id):
                flash("Customer not found.", "danger")
                return redirect(url_for("dashboard"))

            seller_id_raw = request.form.get("seller_id") or ""
            seller_id = None
            if seller_id_raw:
                try:
                    seller_id = int(seller_id_raw)
                except ValueError:
                    seller_id = None

            status_value = _form_text("status") or "pending"
            total_raw = _form_text("total_amount") or "0"
            try:
                total_amount = float(total_raw)
            except ValueError:
                flash("Order total must be numeric.", "warning")
                return redirect(url_for("dashboard"))

            create_order(
                customer_id,
                seller_id=seller_id,
                status=status_value,
                total_amount=total_amount,
                notes=_form_text("notes") or None,
            )
            flash("Order recorded.", "success")
            return redirect(url_for("dashboard"))

        if action == "update_order":
            try:
                order_id = int(request.form.get("order_id", ""))
            except (TypeError, ValueError):
                flash("Could not resolve the order.", "danger")
                return redirect(url_for("dashboard"))

            if not get_order(order_id):
                flash("Order not found.", "danger")
                return redirect(url_for("dashboard"))

            seller_id_raw = request.form.get("seller_id") or ""
            seller_id = None
            if seller_id_raw:
                try:
                    seller_id = int(seller_id_raw)
                except ValueError:
                    seller_id = None

            total_raw = _form_text("total_amount") or ""
            total_value: Optional[float] = None
            if total_raw:
                try:
                    total_value = float(total_raw)
                except ValueError:
                    flash("Order total must be numeric.", "warning")
                    return redirect(url_for("dashboard"))

            update_order(
                order_id,
                status=_form_text("status") or None,
                total_amount=total_value,
                notes=_form_text("notes") or None,
                seller_id=seller_id,
            )
            flash("Order updated.", "success")
            return redirect(url_for("dashboard"))

        if action == "delete_order":
            try:
                order_id = int(request.form.get("order_id", ""))
            except (TypeError, ValueError):
                flash("Could not resolve the order to delete.", "danger")
            else:
                delete_order(order_id)
                flash("Order removed.", "info")
            return redirect(url_for("dashboard"))

        flash("Unknown admin action.", "warning")
        return redirect(url_for("dashboard"))

    customers = fetch_customers()
    orders = fetch_orders()
    sellers = fetch_sellers()
    users = fetch_users()

    return render_template(
        "admin.html",
        customers=customers,
        orders=orders,
        sellers=sellers,
        users=users,
        status_choices=status_choices,
    )


if __name__ == "__main__":
    admin_app.run(debug=True, port=5001)
