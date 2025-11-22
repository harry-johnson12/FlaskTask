"""Public-facing Flask application for the Gearloom storefront."""

from __future__ import annotations

import json
import math
import os
import random
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union, cast
from uuid import uuid4

from flask import (
    Flask,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)

from werkzeug.wrappers import Response
from werkzeug.utils import secure_filename

from openai import OpenAI

from database import (
    create_order,
    create_user,
    create_seller_profile,
    delete_product,
    fetch_product_categories,
    fetch_product_reviews,
    fetch_products,
    fetch_products_by_ids,
    fetch_recent_products_for_user,
    fetch_user_cart,
    format_order_reference,
    fetch_orders_for_user,
    get_order,
    get_product,
    get_product_rating_summary,
    get_seller_by_user_id,
    get_user_by_id,
    get_user_by_username,
    get_user_review,
    init_db,
    insert_product,
    replace_user_cart,
    update_order,
    update_product,
    upsert_product_review,
    upsert_recent_product_view,
)
from security import hash_password, verify_password

# Ensure the database and seed data exist before serving.
init_db()

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret-key-change-me"
UPLOAD_DIR = Path(__file__).with_name("static").joinpath("uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
CHECKOUT_FORM_SESSION_KEY = "checkout_form_data"
CHECKOUT_FORM_FIELDS = (
    "contact_name",
    "contact_email",
    "shipping_address1",
    "shipping_address2",
    "shipping_city",
    "shipping_region",
    "shipping_postal",
    "shipping_country",
    "order_notes",
)
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
COUNTRY_OPTIONS = [
    "United States",
    "Canada",
    "United Kingdom",
    "Germany",
    "France",
    "Australia",
    "New Zealand",
    "Singapore",
    "Japan",
    "India",
    "Brazil",
    "Mexico",
]
REGION_OPTIONS = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "District of Columbia",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
]
COUNTRY_REGION_MAP: dict[str, list[str]] = {
    "United States": REGION_OPTIONS,
    "Canada": [
        "Alberta",
        "British Columbia",
        "Manitoba",
        "New Brunswick",
        "Newfoundland and Labrador",
        "Nova Scotia",
        "Ontario",
        "Prince Edward Island",
        "Quebec",
        "Saskatchewan",
        "Northwest Territories",
        "Nunavut",
        "Yukon",
    ],
    "United Kingdom": [
        "England",
        "Scotland",
        "Wales",
        "Northern Ireland",
        "Isle of Man",
        "Channel Islands",
    ],
    "Germany": [
        "Baden-Wurttemberg",
        "Bavaria",
        "Berlin",
        "Brandenburg",
        "Bremen",
        "Hamburg",
        "Hesse",
        "Lower Saxony",
        "Mecklenburg-Vorpommern",
        "North Rhine-Westphalia",
        "Rhineland-Palatinate",
        "Saarland",
        "Saxony",
        "Saxony-Anhalt",
        "Schleswig-Holstein",
        "Thuringia",
    ],
    "France": [
        "Auvergne-Rhone-Alpes",
        "Bourgogne-Franche-Comte",
        "Bretagne",
        "Centre-Val de Loire",
        "Corse",
        "Grand Est",
        "Hauts-de-France",
        "Ile-de-France",
        "Normandie",
        "Nouvelle-Aquitaine",
        "Occitanie",
        "Pays de la Loire",
        "Provence-Alpes-Cote d'Azur",
    ],
    "Australia": [
        "Australian Capital Territory",
        "New South Wales",
        "Northern Territory",
        "Queensland",
        "South Australia",
        "Tasmania",
        "Victoria",
        "Western Australia",
    ],
    "New Zealand": [
        "Auckland",
        "Bay of Plenty",
        "Canterbury",
        "Gisborne",
        "Hawke's Bay",
        "Manawatu-Wanganui",
        "Marlborough",
        "Nelson",
        "Northland",
        "Otago",
        "Southland",
        "Taranaki",
        "Tasman",
        "Waikato",
        "Wellington",
        "West Coast",
    ],
    "Singapore": ["Singapore"],
    "Japan": [
        "Hokkaido",
        "Aomori",
        "Iwate",
        "Miyagi",
        "Akita",
        "Yamagata",
        "Fukushima",
        "Ibaraki",
        "Tochigi",
        "Gunma",
        "Saitama",
        "Chiba",
        "Tokyo",
        "Kanagawa",
        "Niigata",
        "Toyama",
        "Ishikawa",
        "Fukui",
        "Yamanashi",
        "Nagano",
        "Gifu",
        "Shizuoka",
        "Aichi",
        "Mie",
        "Shiga",
        "Kyoto",
        "Osaka",
        "Hyogo",
        "Nara",
        "Wakayama",
        "Tottori",
        "Shimane",
        "Okayama",
        "Hiroshima",
        "Yamaguchi",
        "Tokushima",
        "Kagawa",
        "Ehime",
        "Kochi",
        "Fukuoka",
        "Saga",
        "Nagasaki",
        "Kumamoto",
        "Oita",
        "Miyazaki",
        "Kagoshima",
        "Okinawa",
    ],
    "India": [
        "Andhra Pradesh",
        "Arunachal Pradesh",
        "Assam",
        "Bihar",
        "Chhattisgarh",
        "Goa",
        "Gujarat",
        "Haryana",
        "Himachal Pradesh",
        "Jharkhand",
        "Karnataka",
        "Kerala",
        "Madhya Pradesh",
        "Maharashtra",
        "Manipur",
        "Meghalaya",
        "Mizoram",
        "Nagaland",
        "Odisha",
        "Punjab",
        "Rajasthan",
        "Sikkim",
        "Tamil Nadu",
        "Telangana",
        "Tripura",
        "Uttar Pradesh",
        "Uttarakhand",
        "West Bengal",
        "Andaman and Nicobar Islands",
        "Chandigarh",
        "Dadra and Nagar Haveli and Daman and Diu",
        "Delhi",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry",
    ],
    "Brazil": [
        "Acre",
        "Alagoas",
        "Amapa",
        "Amazonas",
        "Bahia",
        "Ceara",
        "Distrito Federal",
        "Espirito Santo",
        "Goias",
        "Maranhao",
        "Mato Grosso",
        "Mato Grosso do Sul",
        "Minas Gerais",
        "Para",
        "Paraiba",
        "Parana",
        "Pernambuco",
        "Piaui",
        "Rio de Janeiro",
        "Rio Grande do Norte",
        "Rio Grande do Sul",
        "Rondonia",
        "Roraima",
        "Santa Catarina",
        "Sao Paulo",
        "Sergipe",
        "Tocantins",
    ],
    "Mexico": [
        "Aguascalientes",
        "Baja California",
        "Baja California Sur",
        "Campeche",
        "Chiapas",
        "Chihuahua",
        "Coahuila",
        "Colima",
        "Durango",
        "Guanajuato",
        "Guerrero",
        "Hidalgo",
        "Jalisco",
        "Mexico City",
        "State of Mexico",
        "Michoacan",
        "Morelos",
        "Nayarit",
        "Nuevo Leon",
        "Oaxaca",
        "Puebla",
        "Queretaro",
        "Quintana Roo",
        "San Luis Potosi",
        "Sinaloa",
        "Sonora",
        "Tabasco",
        "Tamaulipas",
        "Tlaxcala",
        "Veracruz",
        "Yucatan",
        "Zacatecas",
    ],
}

# OpenAI configuration for the AI Project Builder feature.
OPENAI_KEY_FILE = Path(__file__).with_name("openai_key.txt")


def _read_openai_key_from_file(path: Path) -> str | None:
    """Read a whitespace-trimmed API key from a local file if present."""

    try:
        contents = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    return contents or None


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or _read_openai_key_from_file(OPENAI_KEY_FILE)
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
PROJECT_BUILDER_INTRO = (
    "Describe the robot, test rig, or power system you are planning and the AI will pull compatible "
    "controllers, sensors, and support gear directly from today's catalogue."
)
PROJECT_BUILDER_CATALOG_LIMIT = 0
PROJECT_BUILDER_MAX_QUANTITY = 6
_openai_client: OpenAI | None = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

HERO_VARIANTS: List[Dict[str, str]] = [
    {
        "badge": "Gearloom Supply",
        "title": "Blueprint-to-basket hardware for ambitious teams.",
        "subtitle": (
            "Gearloom curates mission-critical electronics — spanning robotics brains, edge AI, energy resilience, "
            "and lab tooling — so the upcoming AI build assistant can turn project briefs into ready-to-order kits."
        ),
    },
    {
        "badge": "Field-ready supply",
        "title": "Flight-qualified compute and power blocks without lead-time roulette.",
        "subtitle": (
            "Tap stocked edge brains, calibrated sensor clusters, and battery-safe power planes that ship after "
            "signal-integrity and thermal sweeps."
        ),
    },
    {
        "badge": "Gearloom Supply",
        "title": "Prototype faster with a supply chain that speaks schematics.",
        "subtitle": (
            "We pre-bundle controllers, firmware notes, and harness plans so your lab can jump from CAD to cart with "
            "zero spreadsheet archaeology."
        ),
    },
]


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS


def _save_image(file_storage) -> Optional[str]:
    """Persist an uploaded image with a unique name and return the relative path."""
    if not file_storage or not file_storage.filename:
        return None
    filename = secure_filename(file_storage.filename)
    if not filename or not _allowed_file(filename):
        raise ValueError("Please upload a PNG, JPG, GIF, or WEBP image.")

    extension = Path(filename).suffix
    unique_name = f"{uuid4().hex}{extension}"
    destination = UPLOAD_DIR / unique_name
    file_storage.save(destination)
    return f"uploads/{unique_name}"


def _delete_image(path: Optional[str]) -> None:
    if not path:
        return
    if not path.startswith("uploads/"):
        return
    file_path = Path(__file__).with_name("static").joinpath(path)
    try:
        file_path.unlink()
    except FileNotFoundError:
        return


def _parse_numeric_fields(
    price_raw: str,
    inventory_raw: str,
) -> tuple[Optional[float], Optional[int], Optional[str]]:
    """Convert text fields into typed values while sharing validation messaging."""
    try:
        price_value = float(price_raw)
        inventory_value = int(inventory_raw or 0)
    except ValueError:
        return None, None, "Price must be a number and inventory must be a whole number."
    return price_value, inventory_value, None


def _form_text(field_name: str, default: str = "") -> str:
    """Trim whitespace from form submissions while providing a default."""
    raw_value = request.form.get(field_name)
    if raw_value is None:
        return default
    return raw_value.strip()


def _is_valid_email(value: str) -> bool:
    """Basic validation to ensure the string resembles an email address."""
    if not value:
        return False
    return bool(EMAIL_PATTERN.match(value))


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


def _current_seller() -> Mapping[str, object] | None:
    """Return the seller profile for the logged-in user, if any."""
    user_id = _current_user_id()
    if not user_id:
        return None
    seller = get_seller_by_user_id(user_id)
    return seller


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


def _cart_snapshot(cart: Optional[Dict[int, int]] = None) -> tuple[list[dict[str, object]], float]:
    """Return hydrated cart items with pricing."""

    active_cart = dict(cart) if cart is not None else _get_cart()
    if not active_cart:
        return [], 0.0

    ordered_ids: list[int] = []
    quantity_lookup: Dict[int, int] = {}
    for raw_product_id, raw_quantity in active_cart.items():
        try:
            product_id = int(raw_product_id)
            quantity = int(raw_quantity)
        except (TypeError, ValueError):
            continue
        if product_id not in quantity_lookup:
            ordered_ids.append(product_id)
        quantity_lookup[product_id] = quantity_lookup.get(product_id, 0) + max(0, quantity)

    products = fetch_products_by_ids(ordered_ids)
    product_lookup = {int(product["id"]): product for product in products}
    items: list[dict[str, object]] = []
    total = 0.0

    for product_id in ordered_ids:
        product = product_lookup.get(product_id)
        if not product:
            continue
        quantity = max(0, quantity_lookup.get(product_id, 0))
        if quantity <= 0:
            continue
        price = float(product["price"])
        line_total = price * quantity
        total += line_total
        items.append(
            {
                "product": product,
                "quantity": quantity,
                "line_total": line_total,
            }
        )

    return items, round(total, 2)


def _checkout_form_defaults() -> dict[str, str]:
    """Return the most recent checkout form attempt or sensible defaults."""

    defaults = {field: "" for field in CHECKOUT_FORM_FIELDS}
    saved = session.pop(CHECKOUT_FORM_SESSION_KEY, None)
    if isinstance(saved, dict):
        for field in CHECKOUT_FORM_FIELDS:
            value = saved.get(field)
            if value is None:
                continue
            defaults[field] = str(value)

    user = _current_user()
    if user and not defaults["contact_name"]:
        defaults["contact_name"] = str(user["username"])
    if not defaults["shipping_country"]:
        defaults["shipping_country"] = "United States"
    return defaults


def _remember_checkout_form_submission() -> None:
    """Persist the latest checkout form values so the template can refill them."""

    session[CHECKOUT_FORM_SESSION_KEY] = {
        field: request.form.get(field, "") for field in CHECKOUT_FORM_FIELDS
    }


def _restock_order_inventory(order: Mapping[str, object]) -> None:
    """Return reserved quantities back to the catalogue."""

    items = order.get("items") or []
    if not isinstance(items, list):
        return

    for entry in items:
        if not isinstance(entry, Mapping):
            continue
        product_id = entry.get("product_id")
        quantity = entry.get("quantity", 0)
        try:
            pid = int(product_id)
            qty = int(quantity)
        except (TypeError, ValueError):
            continue
        if pid <= 0 or qty <= 0:
            continue
        product = get_product(pid)
        if not product:
            continue
        current_inventory = int(product.get("inventory_count") or 0)
        update_product(pid, inventory_count=current_inventory + qty)


def _safe_load_recommendation_json(raw_text: str) -> dict[str, Any]:
    """Extract JSON content from an LLM response, trimming any surrounding prose."""

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("Model response did not include JSON data.")
        snippet = raw_text[start : end + 1]
        return json.loads(snippet)


def _short_description(text: str, limit: int = 140) -> str:
    """Return a single-line excerpt for UI display."""

    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit].rstrip()}…"


def _generate_project_builder_recommendations(
    prompt: str,
) -> tuple[list[dict[str, int]], dict[str, str], Optional[str]]:
    """Call the OpenAI Responses API to map a project brief to catalog product ids."""

    prompt_text = (prompt or "").strip()
    if not prompt_text:
        return [], {}, "Describe your project so the builder can help."

    products = list(fetch_products())
    if not products:
        return [], {}, "No products are available to recommend right now."

    catalog_slice = products
    if PROJECT_BUILDER_CATALOG_LIMIT and PROJECT_BUILDER_CATALOG_LIMIT > 0:
        catalog_slice = products[:PROJECT_BUILDER_CATALOG_LIMIT]

    if _openai_client is None:
        return (
            [],
            {},
            "Set the OPENAI_API_KEY environment variable (or add openai_key.txt) to enable AI recommendations.",
        )

    catalog_payload = [
        {
            "id": int(prod["id"]),
            "name": prod["name"],
            "category": prod.get("category"),
            "brand": prod.get("brand"),
            "description": prod.get("description"),
            "price": float(prod.get("price") or 0),
            "inventory_count": int(prod.get("inventory_count") or 0),
        }
        for prod in catalog_slice
    ]

    message_payload = [
        {
            "role": "system",
            "content": [
                {
                    "type": "input_text",
                    "text": (
                        "You are an electronics inventory planner that converts project descriptions into ready-to-order kits. "
                        "Choose only product_id values that exist in catalog_data. "
                        "Return STRICT JSON matching:"
                        ' {"items":[{"product_id":int,"quantity":int,"notes":string}],'
                        '  "insights":{"missing":string,"component_roles":string,"assembly":string}} '
                        f"Quantities must be between 1 and {PROJECT_BUILDER_MAX_QUANTITY} and should respect inventory_count. "
                        "Use the notes field on each item to describe why it matters or what it connects to. "
                        "In insights, summarize what additional gear might be needed (\"missing\"), "
                        "what each cluster of components does (\"component_roles\"), and high-level assembly steps (\"assembly\"). "
                        "If the prompt is broad or only one or two words, infer a practical starter kit for that domain "
                        "(e.g., general-purpose PC/workstation parts, robotics starter stack, sensor kit) and prefer high-stock, versatile parts. "
                        "Only return empty arrays if the catalog truly cannot satisfy the request; otherwise always propose at least one viable bundle."
                    ),
                }
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": f"catalog_data={json.dumps(catalog_payload, separators=(',', ':'))}",
                },
                {"type": "input_text", "text": f"project_prompt={prompt_text}"},
            ],
        },
    ]

    try:
        response = _openai_client.responses.create(model=OPENAI_MODEL, input=message_payload, temperature=0.2)
    except Exception:
        app.logger.exception("OpenAI request failed for the project builder.")
        return [], {}, "We couldn't reach the AI recommender. Please try again."

    raw_text = getattr(response, "output_text", "") or ""
    if not raw_text:
        try:
            chunks: list[str] = []
            for block in getattr(response, "output", []):
                for segment in getattr(block, "content", []):
                    if getattr(segment, "type", None) == "text" and getattr(segment, "text", None):
                        chunks.append(segment.text)
            raw_text = "".join(chunks)
        except Exception:
            raw_text = ""

    if not raw_text:
        return [], {}, "The AI assistant did not return any recommendations."

    try:
        parsed = _safe_load_recommendation_json(raw_text)
    except ValueError as exc:
        app.logger.warning("Unable to parse AI response: %s", exc)
        return [], {}, "The AI response was formatted incorrectly. Please try again."

    items: list[dict[str, int]] = []
    product_lookup = {int(prod["id"]): prod for prod in catalog_slice}
    for entry in parsed.get("items", []):
        try:
            product_id = int(entry.get("product_id") or entry.get("id"))
            quantity = int(entry["quantity"])
        except (KeyError, TypeError, ValueError):
            continue

        if quantity <= 0:
            continue
        matched = product_lookup.get(product_id)
        if not matched:
            continue

        available_stock = max(int(matched.get("inventory_count") or 0), 0)
        if available_stock <= 0:
            continue

        normalized_qty = min(quantity, PROJECT_BUILDER_MAX_QUANTITY, available_stock)
        if normalized_qty <= 0:
            continue

        note = str(entry.get("notes") or "").strip()
        items.append({"product_id": product_id, "quantity": normalized_qty, "notes": note})

    insights_raw = parsed.get("insights") or {}
    insights = {
        "missing": str(insights_raw.get("missing") or "").strip(),
        "component_roles": str(insights_raw.get("component_roles") or "").strip(),
        "assembly": str(insights_raw.get("assembly") or "").strip(),
    }

    if not items:
        return [], insights, "No matching products were available for that project."

    return items, insights, None


def _hydrate_recommendations(recommendations: list[dict[str, int]]) -> tuple[list[dict[str, object]], float]:
    """Return detailed product info for UI rendering plus the subtotal."""

    if not recommendations:
        return [], 0.0

    product_ids = [item["product_id"] for item in recommendations]
    products = fetch_products_by_ids(product_ids)
    product_lookup = {int(prod["id"]): prod for prod in products}

    hydrated: list[dict[str, object]] = []
    subtotal = 0.0

    for item in recommendations:
        product = product_lookup.get(item["product_id"])
        if not product:
            continue
        available_stock = max(int(product.get("inventory_count") or 0), 0)
        quantity = min(int(item["quantity"]), available_stock)
        if quantity <= 0:
            continue

        unit_price = float(product.get("price") or 0.0)
        subtotal += unit_price * quantity
        hydrated.append(
            {
                "id": int(product["id"]),
                "product_id": int(product["id"]),
                "name": product["name"],
                "description": _short_description(str(product["description"])),
                "unit_price": unit_price,
                "quantity": quantity,
                "notes": item.get("notes") or "",
            }
        )

    return hydrated, subtotal


@app.context_processor
def inject_cart_meta() -> Dict[str, object]:
    """Expose cart statistics and user details to every template."""
    cart = _get_cart()
    total_items = sum(cart.values())

    current_user = _current_user()

    return {
        "cart_item_count": total_items,
        "current_user": current_user,
        "current_seller": _current_seller(),
    }


@app.route("/")
def index() -> str:
    """Landing page with a snapshot of highlighted products."""
    products = list(fetch_products())
    sample_count = min(4, len(products))
    featured_products: List[Mapping[str, object]] = random.sample(products, sample_count) if sample_count else []

    left_count = math.ceil(sample_count / 2) if sample_count else 0
    left_products = featured_products[:left_count]
    right_products = featured_products[left_count:]

    hero_content = random.choice(HERO_VARIANTS)

    return render_template(
        "index.html",
        featured_products=featured_products,
        left_products=left_products,
        right_products=right_products,
        hero_content=hero_content,
        project_builder_intro=PROJECT_BUILDER_INTRO,
    )


@app.post("/project-builder/recommend")
def project_builder_recommend():
    """Return AI-powered product recommendations for the provided project prompt."""

    if not _current_user():
        return (
            jsonify({"success": False, "error": "Sign in to build a project cart.", "items": [], "subtotal": 0.0}),
            401,
        )

    payload = request.get_json(silent=True) or {}
    prompt = (payload.get("prompt") or "").strip()

    recommendations, insights, error = _generate_project_builder_recommendations(prompt)
    hydrated, subtotal = _hydrate_recommendations(recommendations)

    success = bool(hydrated) and error is None
    if not success and error is None:
        error = "No matching products were found. Try refining your description."

    return jsonify(
        {
            "success": success,
            "error": None if success else error,
            "insights": insights if success else {},
            "items": hydrated,
            "subtotal": round(subtotal, 2),
        }
    )


@app.post("/project-builder/add-to-cart")
def project_builder_add_to_cart():
    """Add all recommended items to the current user's cart using the existing cart helpers."""

    if not _current_user():
        return jsonify({"success": False, "error": "Sign in to manage your cart."}), 401

    payload = request.get_json(silent=True) or {}
    raw_items = payload.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        return jsonify({"success": False, "error": "No items were provided."}), 400

    cart = _get_cart()
    additions: list[dict[str, object]] = []
    warnings: list[str] = []

    for entry in raw_items:
        try:
            product_id = int(entry["product_id"])
            quantity = int(entry["quantity"])
        except (KeyError, TypeError, ValueError):
            continue

        if quantity <= 0:
            continue

        product = get_product(product_id)
        if not product:
            warnings.append(f"Product {product_id} is no longer available.")
            continue

        available_stock = max(int(product.get("inventory_count") or 0), 0)
        if available_stock <= 0:
            warnings.append(f"{product['name']} is out of stock.")
            continue

        existing_qty = cart.get(product_id, 0)
        space_left = max(available_stock - existing_qty, 0)
        if space_left <= 0:
            warnings.append(f"{product['name']} is already at its available quantity in your cart.")
            continue

        applied_qty = min(space_left, quantity)
        if applied_qty <= 0:
            continue

        cart[product_id] = existing_qty + applied_qty
        additions.append({"id": product_id, "name": product["name"], "quantity": applied_qty})

    if additions:
        _store_cart(cart)
        return jsonify(
            {
                "success": True,
                "items_added": additions,
                "warnings": warnings,
                "cart_item_count": sum(cart.values()),
            }
        )

    error_message = warnings[-1] if warnings else "No products were added to your cart."
    return jsonify({"success": False, "error": error_message, "warnings": warnings, "cart_item_count": sum(cart.values())})


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


@app.route("/service-worker.js")
def service_worker() -> Response:
    """Serve the PWA service worker script from the static directory."""
    return send_from_directory(app.static_folder, "service-worker.js", mimetype="application/javascript")


@app.route("/.well-known/appspecific/com.chrome.devtools.json")
def chrome_devtools_manifest() -> Response:
    """Provide an empty manifest to quiet Chrome DevTools lookups."""
    return jsonify({})


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

        password_hash = hash_password(password)
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
        if not user or not verify_password(password, cast(str, user["password_hash"])):
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
    recently_viewed = fetch_recent_products_for_user(user_id, limit=3) if user_id else []
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
        upsert_recent_product_view(user_id, product_id, max_items=3)

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
        available_stock = int(product.get("inventory_count") or 0)
        if available_stock <= 0:
            flash("This product is currently out of stock.", "warning")
            return redirect(url_for("product_detail", product_id=product_id))

        existing_qty = cart.get(product_id, 0)
        if existing_qty >= available_stock:
            flash("You've already added the maximum available stock for this item.", "info")
            return redirect(url_for("product_detail", product_id=product_id))

        new_total = existing_qty + quantity_value
        if new_total > available_stock:
            new_total = available_stock
            flash(
                f"Only {available_stock} unit{'s' if available_stock != 1 else ''} of {product['name']} are in stock. "
                "Your cart has been updated to the maximum allowed.",
                "warning",
            )
        else:
            flash(f"Added {quantity_value} × {product['name']} to your cart.", "success")

        cart[product_id] = new_total
        _store_cart(cart)
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


@app.route("/seller/dashboard", methods=["GET", "POST"])
def seller_dashboard() -> str | Response:
    """Allow authenticated sellers to manage their slice of the catalogue."""
    user_id = _current_user_id()
    if not user_id:
        flash("Sign in to access the seller tools.", "warning")
        return redirect(url_for("login", next=url_for("seller_dashboard")))

    seller = _current_seller()
    category_options_raw = fetch_product_categories()
    category_options: list[str] = list(dict.fromkeys(category_options_raw))
    if "General" not in category_options:
        category_options.insert(0, "General")
    create_form = {
        "name": "",
        "brand": "",
        "description": "",
        "price": "",
        "sku": "",
        "inventory_count": "",
        "category": category_options[0] if category_options else "General",
    }

    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "register_seller":
            if seller:
                flash("You're already registered as a seller.", "info")
            else:
                store_name = _form_text("store_name")
                contact_email = _form_text("contact_email")
                description = _form_text("description")
                if not store_name:
                    flash("Store name is required to register as a seller.", "warning")
                else:
                    create_seller_profile(
                        user_id,
                        store_name,
                        description=description or None,
                        contact_email=contact_email or None,
                    )
                    flash("Seller profile created. You can now add products.", "success")
                    return redirect(url_for("seller_dashboard"))

        elif action == "create_product":
            if not seller:
                flash("Create a seller profile before adding products.", "warning")
                return redirect(url_for("seller_dashboard"))

            selected_category = _form_text("category") or "General"
            custom_category = _form_text("category_custom")
            category_value = custom_category if selected_category == "__custom__" and custom_category else selected_category
            if not category_value:
                category_value = "General"

            create_form = {
                "name": _form_text("name"),
                "brand": _form_text("brand"),
                "description": _form_text("description"),
                "price": _form_text("price"),
                "sku": _form_text("sku"),
                "inventory_count": _form_text("inventory_count"),
                "category": category_value,
            }

            if not (create_form["name"] and create_form["description"] and create_form["price"]):
                flash("Name, description, and price are required.", "warning")
            else:
                price_value, inventory_value, numeric_error = _parse_numeric_fields(
                    create_form["price"], create_form["inventory_count"]
                )
                if numeric_error:
                    flash(numeric_error, "warning")
                else:
                    try:
                        image_path = _save_image(request.files.get("image"))
                    except ValueError as exc:
                        flash(str(exc), "warning")
                    else:
                        insert_product(
                            create_form["name"],
                            create_form["description"],
                            price_value if price_value is not None else 0.0,
                            brand=create_form["brand"] or "Unbranded",
                            sku=create_form["sku"] or None,
                            inventory_count=inventory_value if inventory_value is not None else 0,
                            image_path=image_path,
                            category=category_value,
                            seller_id=int(cast(int, seller["id"])),
                        )
                        flash("Product added to your catalogue.", "success")
                        return redirect(url_for("seller_dashboard"))

        elif action in {"update_product", "delete_product"}:
            if not seller:
                flash("Create a seller profile before managing products.", "warning")
                return redirect(url_for("seller_dashboard"))
            try:
                product_id = int(request.form.get("product_id", ""))
            except (TypeError, ValueError):
                flash("Could not determine which product to update.", "danger")
                return redirect(url_for("seller_dashboard"))

            product = get_product(product_id)
            seller_id = int(cast(int, seller["id"]))
            if not product or int(product.get("seller_id") or 0) != seller_id:
                flash("You can only manage products that belong to you.", "danger")
                return redirect(url_for("seller_dashboard"))

            if action == "delete_product":
                _delete_image(cast(Optional[str], product.get("image_path")))
                delete_product(product_id)
                flash("Product removed.", "info")
                return redirect(url_for("seller_dashboard"))

            # Update flow
            name = _form_text("name") or str(product["name"])
            brand_value = _form_text("brand") or str(product.get("brand", ""))
            description = _form_text("description") or str(product["description"])
            price_raw = _form_text("price") or f"{product['price']}"
            sku = _form_text("sku") or cast(Optional[str], product.get("sku"))
            inventory_raw = _form_text("inventory_count") or str(product["inventory_count"])
            category_value = _form_text("category") or str(product.get("category", "General"))
            price_value, inventory_value, numeric_error = _parse_numeric_fields(price_raw, inventory_raw)
            if numeric_error:
                flash(numeric_error, "warning")
                return redirect(url_for("seller_dashboard"))

            current_image = cast(Optional[str], product.get("image_path"))
            try:
                new_image = _save_image(request.files.get("image"))
            except ValueError as exc:
                flash(str(exc), "warning")
                return redirect(url_for("seller_dashboard"))

            image_path = current_image
            if new_image:
                if current_image and current_image != new_image:
                    _delete_image(current_image)
                image_path = new_image

            update_product(
                product_id,
                name=name,
                brand=brand_value,
                description=description,
                price=price_value if price_value is not None else float(str(product["price"])),
                sku=sku,
                inventory_count=inventory_value if inventory_value is not None else int(str(product["inventory_count"])),
                image_path=image_path,
                category=category_value,
            )
            flash("Product updated.", "success")
            return redirect(url_for("seller_dashboard"))

        else:
            flash("Unknown seller action.", "warning")

    seller_refresh = _current_seller()
    seller_products = []
    if seller_refresh:
        seller_products = list(fetch_products(seller_id=int(cast(int, seller_refresh["id"]))))

    return render_template(
        "seller_dashboard.html",
        seller=seller_refresh,
        create_form=create_form,
        products=seller_products,
        category_options=category_options,
    )


@app.route("/cart")
def cart() -> str:
    """Display the current basket with totals."""
    items, total = _cart_snapshot()
    checkout_defaults = _checkout_form_defaults()
    selected_country = checkout_defaults.get("shipping_country") or "United States"
    region_options = COUNTRY_REGION_MAP.get(selected_country, [])
    return render_template(
        "cart.html",
        items=items,
        total=total,
        checkout_defaults=checkout_defaults,
        country_options=COUNTRY_OPTIONS,
        region_options=region_options,
        region_map=COUNTRY_REGION_MAP,
    )


@app.route("/orders")
def orders_history() -> str:
    """Allow authenticated users to review their recent orders."""
    user_id = _current_user_id()
    if not user_id:
        flash("Sign in to view your orders.", "warning")
        return redirect(url_for("login", next=url_for("orders_history")))

    orders = fetch_orders_for_user(user_id)
    return render_template("orders.html", orders=orders)


@app.post("/orders/<int:order_id>/cancel")
def cancel_order(order_id: int):
    """Allow a user to cancel their pending reservation."""

    user_id = _current_user_id()
    if not user_id:
        flash("Sign in to manage orders.", "warning")
        return redirect(url_for("login", next=url_for("orders_history")))

    order = get_order(order_id)
    if not order or int(order.get("user_id") or 0) != user_id:
        flash("We couldn't find that order.", "danger")
        return redirect(url_for("orders_history"))

    if order.get("status") != "pending":
        flash("Only pending reservations can be canceled.", "warning")
        return redirect(url_for("orders_history"))

    update_order(order_id, status="cancelled")
    _restock_order_inventory(order)
    reference = order.get("reference") or format_order_reference(order_id)
    flash(f"{reference} was canceled and inventory returned to stock.", "info")
    return redirect(url_for("orders_history"))


@app.post("/checkout")
def checkout():
    """Capture checkout details and persist an order snapshot."""

    user_id = _current_user_id()
    if not user_id:
        flash("Sign in to submit an order.", "warning")
        return redirect(url_for("login", next=url_for("cart")))

    cart = _get_cart()
    items, total = _cart_snapshot(cart)
    if not items:
        flash("Add at least one product to your cart before checking out.", "warning")
        return redirect(url_for("cart"))

    contact_name = _form_text("contact_name")
    contact_email = _form_text("contact_email")
    address_line_one = _form_text("shipping_address1")
    address_line_two = _form_text("shipping_address2")
    shipping_city = _form_text("shipping_city")
    shipping_region = _form_text("shipping_region")
    shipping_postal = _form_text("shipping_postal")
    shipping_country = _form_text("shipping_country") or "United States"
    order_notes = _form_text("order_notes")

    errors: list[str] = []
    if not contact_name:
        errors.append("Enter the recipient name so we can address the shipment.")
    if not contact_email or not _is_valid_email(contact_email):
        errors.append("Provide a valid email address for order updates.")
    if not address_line_one:
        errors.append("Add a shipping address line.")
    if not shipping_city:
        errors.append("Specify the city for delivery.")
    if not shipping_postal:
        errors.append("Postal or ZIP code is required.")
    if not shipping_country:
        errors.append("Include the destination country.")
    if not shipping_region:
        errors.append("Select the state or region for the destination.")
    allowed_regions = COUNTRY_REGION_MAP.get(shipping_country)
    if allowed_regions and shipping_region and shipping_region not in allowed_regions:
        errors.append("Pick a region that matches the selected country.")

    if errors:
        _remember_checkout_form_submission()
        for message in errors:
            flash(message, "warning")
        return redirect(url_for("cart"))

    inventory_messages: list[str] = []
    for entry in items:
        product = entry["product"]
        product_id = int(product["id"])
        available_stock = int(product.get("inventory_count") or 0)
        requested = int(entry["quantity"])
        if available_stock <= 0:
            cart.pop(product_id, None)
            inventory_messages.append(f"{product['name']} is no longer in stock and was removed from your cart.")
        elif requested > available_stock:
            cart[product_id] = available_stock
            inventory_messages.append(
                f"{product['name']} has only {available_stock} in stock. Your cart was updated to the maximum."
            )

    if inventory_messages:
        _store_cart(cart)
        _remember_checkout_form_submission()
        for message in inventory_messages:
            flash(message, "warning")
        flash("Review the updated cart before checking out.", "warning")
        return redirect(url_for("cart"))

    seller_ids: set[int] = set()
    order_items_payload: list[dict[str, object]] = []
    for entry in items:
        product = entry["product"]
        quantity = int(entry["quantity"])
        try:
            seller_value = product.get("seller_id")
            if seller_value is not None:
                seller_ids.add(int(seller_value))
        except (TypeError, ValueError):
            pass
        order_items_payload.append(
            {
                "product_id": int(product["id"]),
                "product_name": str(product["name"]),
                "product_sku": product.get("sku"),
                "quantity": quantity,
                "unit_price": float(product["price"]),
            }
        )

    shipping_address = "\n".join(line for line in [address_line_one, address_line_two] if line)
    seller_id_value = seller_ids.pop() if len(seller_ids) == 1 else None
    total_amount = round(total, 2)

    order_id = create_order(
        user_id=user_id,
        seller_id=seller_id_value,
        status="pending",
        total_amount=total_amount,
        notes=order_notes or None,
        contact_name=contact_name,
        contact_email=contact_email,
        shipping_address=shipping_address,
        shipping_city=shipping_city,
        shipping_region=shipping_region or None,
        shipping_postal=shipping_postal,
        shipping_country=shipping_country,
        items=order_items_payload,
    )

    for entry in items:
        product = entry["product"]
        product_id = int(product["id"])
        available_stock = int(product.get("inventory_count") or 0)
        new_inventory = max(0, available_stock - int(entry["quantity"]))
        update_product(product_id, inventory_count=new_inventory)

    _store_cart({})
    session.pop(CHECKOUT_FORM_SESSION_KEY, None)
    order_reference = format_order_reference(order_id)
    flash(f"{order_reference} received. Operations will follow up with next steps soon.", "success")
    return redirect(url_for("cart"))


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

    product = get_product(product_id)
    if not product:
        cart.pop(product_id, None)
        _store_cart(cart)
        flash("That product is no longer available and was removed from your cart.", "warning")
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
        available_stock = int(product.get("inventory_count") or 0)
        if available_stock <= 0:
            cart.pop(product_id, None)
            flash("That item is no longer in stock and was removed from your cart.", "warning")
        elif quantity_value > available_stock:
            cart[product_id] = available_stock
            flash(
                f"Only {available_stock} unit{'s' if available_stock != 1 else ''} of {product['name']} are available. "
                "Your cart quantity was adjusted.",
                "warning",
            )
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
