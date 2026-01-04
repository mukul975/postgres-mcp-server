import inspect

import postgres_server


def test_creation_tools_exist() -> None:
    assert inspect.iscoroutinefunction(postgres_server.PostgreSQL_create_schema)
    assert inspect.iscoroutinefunction(postgres_server.PostgreSQL_create_table)
    assert inspect.iscoroutinefunction(postgres_server.PostgreSQL_create_index)

