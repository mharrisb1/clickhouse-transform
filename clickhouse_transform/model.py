from pathlib import Path
from typing import Any, Callable

from daglib import Task

from clickhouse_transform.session import Session
from clickhouse_transform.types import Model


def model(alias: str | None = None):
    def wrapper(fn: Callable[..., Any]) -> Task:
        if alias:
            name = alias
        else:
            name = fn.__name__
        return Task.from_function(fn, name)

    return wrapper


def model_from_sql_file(p: str | Path) -> Task:
    p = Path(p)
    if not all([
        p.exists(),
        p.name.split(".")[-1].lower() == "sql",
        p.is_file()
    ]):
        raise FileNotFoundError
    sql = open(p, "r").read()

    def sql_model(session: Session) -> Model:
        return session.sql(sql)

    return Task.from_function(sql_model, p.name.split(".")[0].lower())
