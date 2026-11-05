
"""Read-only PostgreSQL ownership connection diagnostics."""

import time

from dotenv import load_dotenv

from gold.implementation.ownership import (
    connect_gold_lock_db,
)


def main():
    load_dotenv()

    for attempt in range(1, 4):
        start = time.perf_counter()

        conn = connect_gold_lock_db()

        connected = time.perf_counter()

        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()

            queried = time.perf_counter()

            assert result == (1,)

        finally:
            conn.close()

        closed = time.perf_counter()

        print(
            f"Attempt {attempt}: "
            f"connect={connected - start:.3f}s, "
            f"query={queried - connected:.3f}s, "
            f"close={closed - queried:.3f}s, "
            f"total={closed - start:.3f}s"
        )


if __name__ == "__main__":
    main()
