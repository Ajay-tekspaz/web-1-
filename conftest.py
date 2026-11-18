from datetime import date
from pathlib import Path

import pytest

from data_generation.deterministic_generator import GeneratorConfig, generate

# Small but long enough to cover Memorial Day and July 4th, and to fit a weekly-seasonal model.
SMALL = GeneratorConfig(seed=7, start_date=date(2026, 4, 6), days=98, num_stores=3, num_products=12,
                        renamed_column_day=60)


@pytest.fixture(scope="session")
def small_data(tmp_path_factory) -> tuple[Path, object]:
    out = tmp_path_factory.mktemp("generated")
    truth = generate(SMALL, out)
    return out, truth
