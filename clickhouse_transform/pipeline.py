from __future__ import annotations

from pathlib import Path
from functools import partial
from typing import Any, Callable

from daglib import Dag, Task, Arg

from clickhouse_transform.session import Session
from clickhouse_transform.model import model_from_sql_file
from clickhouse_transform.types import Model


def _create_table_method_factory(
        session: Session,
        database: str,
        alias: str,
        engine: str,
        on_cluster: str | None = None,
        partition_by: str | None = None,
        primary_key: str | None = None,
        order_by: str | None = None,
        settings: dict[str, ...] | None = None,
) -> Callable[..., Any]:
    return partial(
        session.create_table_from_model,
        database=database,
        table=alias,
        engine=engine,
        cluster=on_cluster,
        partition_by=partition_by,
        primary_key=primary_key,
        order_by=order_by,
        settings=settings,
    )


class Pipeline:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._dag = Dag()
        self._dag.register_task(Task.from_object(session, "session"))

    def model(
            self,
            *,
            materialize: str = "ephemeral",
            database: str | None = None,
            alias: str | None = None,
            on_cluster: str | None = None,
            engine: str | None = None,
            partition_by: str | None = None,
            primary_key: str | None = None,
            order_by: str | None = None,
            settings: dict[str, ...] | None = None,
    ) -> Any:
        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            if materialize == "ephemeral":
                self._dag.register_task_from_function(fn)
            elif materialize == "table":
                if not engine:
                    raise ValueError("Must specify engine for table creation")

                _alias = alias  # value from outer scope is not mutable
                if not _alias:
                    _alias = fn.__name__

                task = Task.from_function(fn, f"compile_{_alias}_model")
                self._dag.register_task(task)

                create_table_fn = _create_table_method_factory(
                    self.session, database, _alias, engine, on_cluster, partition_by, primary_key, order_by, settings
                )

                task = Task(create_table_fn, name=fn.__name__, args=[Arg(f"compile_{_alias}_model")])
                self._dag.register_task(task)

            else:
                raise ValueError("Materialize must be set to one of 'ephemeral', 'table'")
            return fn

        return register

    def add_models_from_sql(self, path: str | Path) -> Pipeline:
        path = Path(path)
        if path.is_dir():
            for p in path.rglob("*.sql"):
                self._dag.register_task(model_from_sql_file(p))
        elif path.is_file():
            self._dag.register_task(model_from_sql_file(path))
        else:
            raise FileNotFoundError
        return self

    def add_source(self, database: str, table: str, table_alias: str | None = None) -> Pipeline:
        if not table_alias:
            table_alias = table
        task = Task.from_object(self.session.table(database, table), f"{database}_{table_alias}")
        self._dag.register_task(task)
        return self

    def add_model(self, model: Model, name: str) -> Pipeline:
        fn: Callable[..., Model] = lambda: model
        task = Task.from_function(fn, name)
        self._dag.register_task(task)
        return self

    def add_pipeline(self, other: Pipeline) -> Pipeline:
        self._dag.add_subdag(other._dag)
        return self

    def run(self, *args, **kwargs) -> Any:
        return self._dag.run(*args, **kwargs)

    def visualize(self, *args, **kwargs) -> None:
        return self._dag.visualize(*args, **kwargs)
