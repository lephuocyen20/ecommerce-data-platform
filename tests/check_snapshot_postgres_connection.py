
import os

import psycopg2


def main():
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()[0]

        assert result == 1

        print("POSTGRESQL CONNECTION: PASS")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
