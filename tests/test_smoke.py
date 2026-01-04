import inspect
import sys
from pathlib import Path

repository_root = Path(__file__).resolve().parents[1]
if str(repository_root) not in sys.path:
    sys.path.insert(0, str(repository_root))

import postgres_server


def test_creation_tools_exist() -> None:
    assert inspect.iscoroutinefunction(postgres_server.PostgreSQL_create_schema)
    assert inspect.iscoroutinefunction(postgres_server.PostgreSQL_create_table)
    assert inspect.iscoroutinefunction(postgres_server.PostgreSQL_create_index)
