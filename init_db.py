"""
Tiny helper script to create the SQLite database before running the app.
Usage: python init_db.py
"""

from database import init_db, DB_PATH


def main() -> None:
    init_db()
    print(f"Database ready at {DB_PATH}")


if __name__ == "__main__":
    main()

