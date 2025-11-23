# Gearloom Storefront (Flask)

Built by Harry Johnson for his Software Engineering HSC Task 1. A simple ecommerce site with a customer shop, seller tools, and an admin console.

## What’s inside
- `app.py`: customer-facing Flask app (browse, cart, checkout, orders, seller dashboard).
- `admin_app.py`: admin-only Flask app for users, sellers, products, and orders.
- `database.py`: SQLAlchemy models, seed logic, and CRUD helpers against `store.db`.
- `security.py`: bcrypt hashing, password policy checks, and Fernet encryption helpers.
- `templates/`: Jinja templates for storefront, seller, and admin pages.
- `static/`: CSS, logo, service worker, and runtime uploads (`static/uploads` is created automatically).
- `seed_data/product_catalog.py`: seeded product data.
- `seed_data/download_product_images.py`: optional helper to download sample catalog images.

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run
In separate terminals after activating the venv:
```bash
flask --app app run
flask --app admin_app run
```

## Demo logins
- Admin: `ops_admin` / `seed-pass`
- Alternatively, create your own by signing up!

## Config tips
- `openai_key.txt` enables the AI Project Builder; leave unset to disable.
- `SENSITIVE_DATA_KEY` (or `sensitive_key.txt`) holds the Fernet key; auto-generated if missing.
- Delete `store.db` if you would like to reseed demo data.
