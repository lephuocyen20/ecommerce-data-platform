
"""PostgreSQL Gold fencing allocation E2E tests."""

import unittest
from uuid import uuid4

from dotenv import load_dotenv

from gold.implementation.fencing import (
    generate_gold_attempt_id,
)
from gold.implementation.ownership import (
    GoldOwnershipError,
    acquire_gold_ownership,
    connect_gold_lock_db,
)


DATASET = "daily_sales"


class TestGoldFencingRegistryE2E(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_dotenv(dotenv_path=".env")

    def setUp(self):
        self.snapshot_id = f"gold_snap_{uuid4().hex}"

    def test_monotonic_fencing_tokens(self):
        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as first_owner:
            first = first_owner.allocate_fencing_token(
                generate_gold_attempt_id()
            )

            self.assertEqual(first.fencing_token, 1)

        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as second_owner:
            second = second_owner.allocate_fencing_token(
                generate_gold_attempt_id()
            )

            self.assertEqual(second.fencing_token, 2)
            self.assertGreater(
                second.fencing_token,
                first.fencing_token,
            )

    def test_released_owner_cannot_allocate(self):
        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as owner:
            owner.allocate_fencing_token(
                generate_gold_attempt_id()
            )

        with self.assertRaises(GoldOwnershipError):
            owner.allocate_fencing_token(
                generate_gold_attempt_id()
            )

    def test_concurrent_owner_rejected(self):
        from gold.implementation.ownership import (
            GoldOwnershipBusyError,
        )

        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as first_owner:
            first_owner.allocate_fencing_token(
                generate_gold_attempt_id()
            )

            with self.assertRaises(
                GoldOwnershipBusyError
            ):
                with acquire_gold_ownership(
                    DATASET,
                    self.snapshot_id,
                ):
                    pass

    def test_registry_matches_allocated_attempt(self):
        attempt_id = generate_gold_attempt_id()

        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as owner:
            attempt = owner.allocate_fencing_token(
                attempt_id
            )

        conn = connect_gold_lock_db()

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT fencing_token, attempt_id
                    FROM public.gold_execution_fencing
                    WHERE dataset_name = %s
                      AND snapshot_id = %s
                    """,
                    (DATASET, self.snapshot_id),
                )
                row = cursor.fetchone()

            self.assertEqual(
                row,
                (attempt.fencing_token, attempt_id),
            )

        finally:
            conn.close()



    def test_old_attempt_reuse_rejected(self):
        from gold.implementation.fencing import GoldFencingError

        attempt_a = generate_gold_attempt_id()
        attempt_b = generate_gold_attempt_id()

        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as owner:
            first = owner.allocate_fencing_token(attempt_a)
            second = owner.allocate_fencing_token(attempt_b)

            self.assertEqual(first.fencing_token, 1)
            self.assertEqual(second.fencing_token, 2)

            with self.assertRaises(GoldFencingError):
                owner.allocate_fencing_token(attempt_a)

            with owner._conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT fencing_token, attempt_id
                    FROM public.gold_execution_fencing
                    WHERE dataset_name = %s
                      AND snapshot_id = %s
                    """,
                    (DATASET, self.snapshot_id),
                )
                current = cursor.fetchone()

            self.assertEqual(current, (2, attempt_b))

    def test_history_insert_failure_rolls_back_registry(self):
        from gold.implementation.fencing import GoldFencingError

        attempt_id = generate_gold_attempt_id()

        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as owner:
            # Force history INSERT failure using a temporary
            # PostgreSQL trigger on the test snapshot only.
            with owner._conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE OR REPLACE FUNCTION
                    pg_temp.reject_gold_history_insert()
                    RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        RAISE EXCEPTION
                            'Simulated history insertion failure';
                    END;
                    $$;
                    """
                )

                cursor.execute(
                    """
                    CREATE TRIGGER reject_gold_history_test
                    BEFORE INSERT ON
                        public.gold_execution_attempt_history
                    FOR EACH ROW
                    WHEN (
                        NEW.dataset_name = 'daily_sales'
                    )
                    EXECUTE FUNCTION
                        pg_temp.reject_gold_history_insert()
                    """
                )

            try:
                with self.assertRaises(Exception):
                    owner.allocate_fencing_token(attempt_id)

                with owner._conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM public.gold_execution_fencing
                        WHERE dataset_name = %s
                          AND snapshot_id = %s
                        """,
                        (DATASET, self.snapshot_id),
                    )
                    registry_count = cursor.fetchone()[0]

                self.assertEqual(registry_count, 0)

            finally:
                with owner._conn.cursor() as cursor:
                    cursor.execute(
                        """
                        DROP TRIGGER IF EXISTS
                            reject_gold_history_test
                        ON public.gold_execution_attempt_history
                        """
                    )

    def test_registry_history_consistency(self):
        attempt_id = generate_gold_attempt_id()

        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as owner:
            attempt = owner.allocate_fencing_token(attempt_id)

            with owner._conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        f.fencing_token,
                        f.attempt_id,
                        h.fencing_token,
                        h.attempt_id
                    FROM public.gold_execution_fencing f
                    JOIN public.gold_execution_attempt_history h
                      ON f.dataset_name = h.dataset_name
                     AND f.snapshot_id = h.snapshot_id
                     AND f.attempt_id = h.attempt_id
                    WHERE f.dataset_name = %s
                      AND f.snapshot_id = %s
                    """,
                    (DATASET, self.snapshot_id),
                )
                row = cursor.fetchone()

            self.assertEqual(
                row,
                (
                    attempt.fencing_token,
                    attempt_id,
                    attempt.fencing_token,
                    attempt_id,
                ),
            )




    def test_duplicate_current_attempt_rejected(self):
        from gold.implementation.fencing import (
            GoldFencingError,
        )

        attempt_id = generate_gold_attempt_id()

        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as owner:
            first = owner.allocate_fencing_token(
                attempt_id
            )

            self.assertEqual(first.fencing_token, 1)

            with self.assertRaises(GoldFencingError):
                owner.allocate_fencing_token(
                    attempt_id
                )

            # Verify the rejected allocation did not
            # increment the registry token.
            with owner._conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT fencing_token, attempt_id
                    FROM public.gold_execution_fencing
                    WHERE dataset_name = %s
                      AND snapshot_id = %s
                    """,
                    (DATASET, self.snapshot_id),
                )
                row = cursor.fetchone()

            self.assertEqual(row, (1, attempt_id))

    def test_duplicate_attempt_after_reacquire_rejected(self):
        from gold.implementation.fencing import (
            GoldFencingError,
        )

        attempt_id = generate_gold_attempt_id()

        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as first_owner:
            first = first_owner.allocate_fencing_token(
                attempt_id
            )

        with acquire_gold_ownership(
            DATASET,
            self.snapshot_id,
        ) as second_owner:
            with self.assertRaises(GoldFencingError):
                second_owner.allocate_fencing_token(
                    attempt_id
                )

        self.assertEqual(first.fencing_token, 1)



if __name__ == "__main__":
    unittest.main(verbosity=2)
