import random
from datetime import datetime, timedelta

REFERENCE_DATE = datetime(2026, 10, 1, 0, 0, 0)


def random_timestamp(start, end):
    """Generate a random timestamp between two datetimes."""
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

