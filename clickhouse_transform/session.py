from __future__ import annotations

from typing import Any

import ibis

from clickhouse_transform.types import Model
from clickhouse_transform.codegen.ddl import create_table


# noinspection PyPep8Naming
class classproperty(property):
    """
    Taken from
    https://github.com/apache/spark/blob/a6c724ed816bd83cc6b3f5178b462e63b8e85972/python/pyspark/sql/session.py#L114
    """

    def __get__(self, instance: Any, owner: Any = None) -> Any:
        return classmethod(self.fget).__get__(None, owner)()  # type: ignore


class Session:
    def __init__(self, **kwargs) -> None:
        self._configs = kwargs

    def enable_logging(self) -> Session:
        ibis.options.verbose = True
        return self

    class Builder:
        def __init__(self) -> None:
            self._verbose = False
            self._configs: dict[str, ...] = {}

        def from_configs(self, configs: dict[str, ...]) -> Session.Builder:
            self._configs = configs
            return self

        def enable_logging(self) -> Session.Builder:
            self._verbose = True
            return self

        def create(self) -> Session:
            session = Session(**self._configs)
            if self._verbose:
                session = session.enable_logging()
            return session

    # noinspection PyMethodParameters
    @classproperty
    def builder(cls) -> Session.Builder:
        return cls.Builder()

    def table(self, database: str, table: str) -> Model:
        conn = ibis.clickhouse.connect(**self._configs)
        table = conn.table(f"{database}.{table}")
        conn.close()
        return table

    def sql(self, query: str) -> Model:
        conn = ibis.clickhouse.connect(**self._configs)
        sql = conn.sql(query)
        conn.close()
        return sql

    def drop_table(self, database: str, table: str, cluster: str | None = None) -> None:
        on_cluster = f" ON CLUSTER {cluster!r}" if cluster else ""
        conn = ibis.clickhouse.connect(**self._configs)
        conn.raw_sql(f"DROP TABLE IF EXISTS {database}.{table}" + on_cluster)
        conn.close()

    def set_database(self, database: str) -> Session:
        self._configs["database"] = database
        return self

    def list_tables(self) -> list[str]:
        conn = ibis.clickhouse.connect(**self._configs)
        tables = conn.list_tables()
        conn.close()
        return tables

    def create_table_from_model(
        self, model: Model, database: str, table: str, engine: str, cluster: str | None = None, **kwargs
    ) -> Model:
        if table in self.set_database(database).list_tables():
            self.drop_table(database, table, cluster)

        ddl_stmt, insert_stmt = create_table(model, database, table, engine, cluster=cluster, **kwargs)
        conn = ibis.clickhouse.connect(**self._configs)
        conn.raw_sql(ddl_stmt)
        conn.raw_sql(insert_stmt)
        conn.close()
        return self.table(database, table)
