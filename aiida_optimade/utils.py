from typing import Tuple
import urllib.parse

from optimade.models import DataType
from optimade.server.config import CONFIG


OPEN_API_ENDPOINTS = {
    "docs": "/extensions/docs",
    "redoc": "/extensions/redoc",
    "openapi": "/extensions/openapi.json",
}


def retrieve_queryable_properties(
    schema: dict, queryable_properties: list
) -> Tuple[dict, dict]:
    """Get all queryable properties from an OPTIMADE schema"""
    properties = {}
    all_properties = {}

    for name, value in schema["properties"].items():
        if name in queryable_properties:
            if "$ref" in value:
                path = value["$ref"].split("/")[1:]
                sub_schema = schema.copy()
                while path:
                    next_key = path.pop(0)
                    sub_schema = sub_schema[next_key]
                sub_queryable_properties = sub_schema["properties"].keys()
                new_properties, new_all_properties = retrieve_queryable_properties(
                    sub_schema, sub_queryable_properties
                )
                properties.update(new_properties)
                all_properties.update(new_all_properties)
            else:
                all_properties[name] = value
                properties[name] = {"description": value.get("description", "")}
                for extra_key in ["unit"]:
                    if extra_key in value:
                        properties[name][extra_key] = value[extra_key]
                # AiiDA's QueryBuilder can sort everything that isn't a list (array)
                # or dict (object)
                properties[name]["sortable"] = value.get("type", "") not in [
                    "array",
                    "object",
                ]
                # Try to get OpenAPI-specific "format" if possible,
                # else get "type"; a mandatory OpenAPI key.
                properties[name]["type"] = DataType.from_json_type(
                    value.get("format", value["type"])
                )

    return properties, all_properties


def get_custom_base_url_path():
    """Return path part of custom base URL"""
    if CONFIG.base_url is not None:
        res = urllib.parse.urlparse(CONFIG.base_url).path
    else:
        res = urllib.parse.urlparse(CONFIG.base_url).path.decode()

    if res.endswith("/"):
        res = res[:-1]

    return res


def render_query(statement, db_session):
    """
    Generate an SQL expression string with bound parameters rendered inline
    for the given SQLAlchemy statement.
    WARNING: This method of escaping is insecure, incomplete, and for debugging
    purposes only. Executing SQL statements with inline-rendered user values is
    extremely insecure.
    Based on http://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query
    """
    from datetime import date, datetime, timedelta
    from sqlalchemy.orm import Query

    if isinstance(statement, Query):
        statement = statement.statement
    dialect = db_session.bind.dialect

    class LiteralCompiler(dialect.statement_compiler):
        def visit_bindparam(
            self, bindparam, within_columns_clause=False, literal_binds=False, **kwargs
        ):
            return self.render_literal_value(bindparam.value, bindparam.type)

        def render_jsonb_value(self, val, item_type):
            if isinstance(val, list):
                return "{}".format(
                    ",".join([self.render_jsonb_value(x, item_type) for x in val])
                )
            if isinstance(val, str):
                return '"{}"'.format(val)
            return self.render_literal_value(val, item_type)

        def render_literal_value(self, value, type_):
            if isinstance(value, int):
                return str(value)
            elif isinstance(value, (str, date, datetime, timedelta)):
                return "'{}'".format(str(value).replace("'", "''"))
            elif isinstance(value, list):
                return "'[{}]'".format(
                    ",".join([self.render_jsonb_value(x, type_) for x in value])
                )
            return super(LiteralCompiler, self).render_literal_value(value, type_)

    return LiteralCompiler(dialect, statement).process(statement)
