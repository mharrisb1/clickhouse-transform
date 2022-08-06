import ibis

from clickhouse_transform.types import Model


def create_table(
    model: Model,
    database: str,
    name: str,
    engine: str,
    *,
    cluster: str | None = None,
    partition_by: str | None = None,
    order_by: str | None = None,
    primary_key: str | None = None,
    settings: dict[str, ...] | None = None,
) -> tuple[str, str]:
    ddl_stmt = f"CREATE TABLE {database}.{name}"
    if cluster:
        ddl_stmt += f" ON CLUSTER {cluster!r}"
    ddl_stmt += "\n"
    schema = ",\n".join([f"\t{line[0]} {line[1].name}" for line in model.schema().items()])
    ddl_stmt += f"(\n{schema}\n)\n"
    ddl_stmt += f"ENGINE = {engine}\n"
    if partition_by:
        ddl_stmt += f"PARTITION BY {partition_by}\n"
    if order_by:
        ddl_stmt += f"ORDER BY {order_by}\n"
    if primary_key:
        ddl_stmt += f"PRIMARY KEY {primary_key}\n"
    if settings:
        ddl_stmt += "SETTINGS "
        ddl_stmt += ", ".join([f"{k} = {v}" for k, v in settings.items()])
    insert_statement = f"INSERT INTO {database}.{name} (*)\n"
    insert_statement += ibis.clickhouse.compile(model)
    return ddl_stmt, insert_statement
