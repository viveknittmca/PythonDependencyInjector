import logging
import time

import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select, insert, update, delete, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import and_, or_
from prometheus_client import Counter, Histogram
from typing import Optional, Dict, Union, Any, Callable, List

from sqlalchemy.dialects.postgresql import insert as pg_insert
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
DB_QUERY_DURATION = Histogram(
    "db_query_duration_seconds",
    "Time taken to execute DB operations",
    ["operation", "table"]
)

DB_QUERY_ERRORS = Counter(
    "db_query_errors_total",
    "Total DB errors by operation",
    ["operation", "table", "error"]
)
# "postgresql+psycopg2://scott:tiger@localhost/test"
# dialect[+driver]://user:password@host/dbname[?key=value..]


class RelationalDbClient:
    def __init__(
        self,
        db_url: str,
        schema: Optional[str] = None,
        echo: bool = False
    ):
        self.engine: Engine = create_engine(db_url, echo=echo, pool_pre_ping=True, future=True)
        self.schema = schema
        self.meta = MetaData(schema=self.schema)
        self.meta.reflect(bind=self.engine)

    @staticmethod
    def _extract_rows(result) -> List[Dict[str, Any]]:
        if not result.returns_rows:
            return []
        return [dict(row._mapping) for row in result]

    @staticmethod
    def _extract_row_count(result) -> int:
        if not result or not result.rowcount:
            return 0
        return result.rowcount

    @staticmethod
    def _get_table_names_from_stmt(statement) -> str:
        def extract_names(from_obj):
            if hasattr(from_obj, 'name') and isinstance(from_obj.name, str):
                return [from_obj.name]
            elif isinstance(from_obj, sqlalchemy.sql.selectable.Join):
                left = extract_names(from_obj.left)
                right = extract_names(from_obj.right)
                return left + right
            return []

        try:
            tables = []
            for f in statement.get_final_froms():
                tables.extend(extract_names(f))
            return tables
        except RuntimeError:
            return "unknown"

    def execute(self, statement, table: Union[str, List[str]] = None, operation: str = "query"):
        result = self._execute(statement, table, operation)
        return self._extract_rows(result)

    def _execute(self, statement, tables: Union[str, List[str]] = None, operation: str = "query") -> Any:
        # tables = '|'.join(sorted(tables)) if isinstance(tables, List) else tables
        tables = self._get_table_names_from_stmt(statement) if tables is None else tables
        tables = '|'.join(sorted(set(tables))) if isinstance(tables, list) else tables

        logger.info(f"{tables=}")
        start = time.perf_counter()
        try:
            with self.engine.begin() as conn:
                result = conn.execute(statement)
                return result
        except SQLAlchemyError as e:
            DB_QUERY_ERRORS.labels(operation, tables, type(e).__name__).inc()
            logger.warning(f"[DB ERROR] {operation.upper()} {tables} → {type(e).__name__}: {e}")
            raise
        finally:
            duration = time.perf_counter() - start
            DB_QUERY_DURATION.labels(operation, tables).observe(duration)
            if duration > 1.0:  # ⏱️ Log if query takes > 1 second
                logger.warning(f"[SLOW QUERY] {operation.upper()} on '{tables}' took {duration:.2f}s")

    def fetch_one(self, table_name: str, where: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        table = self.meta.tables[table_name]
        conditions = and_(*[table.c[k] == v for k, v in where.items()])
        statement = select(table).where(conditions).limit(1)
        result = self._execute(statement, table_name, "fetch_one")
        return self._extract_rows(result)[0]

    def fetch_all(self, table_name: str, where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        table = self.meta.tables[table_name]
        stmt = select(table)
        if where:
            stmt = stmt.where(and_(*[table.c[k] == v for k, v in where.items()]))
        result = self._execute(stmt, table_name, "fetch_all")
        return self._extract_rows(result)

    # def insert(self, table_name: str, values: Dict[str, Any]) -> None:
    #     table = self.meta.tables[table_name]
    #     stmt = insert(table).values(**values)
    #     self._execute(stmt, table_name, "insert")

    def insert(self, table_name: str, values: Union[Dict[str, Any], List[Dict[str, Any]]]) -> int:
        table = self.meta.tables[table_name]
        stmt = insert(table).values(values)
        result = self._execute(stmt, table_name, operation="insert")
        return self._extract_row_count(result)

    def upsert(
            self,
            table_name: str,
            values: Dict[str, Any],
            conflict_columns: List[str],
            update_columns: List[str] = None,
            do_nothing: bool = False
    ) -> int:
        table = self.meta.tables[table_name]
        stmt = pg_insert(table).values(**values)

        if do_nothing:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
        else:
            update_columns = update_columns or [
                c.name for c in table.columns if c.name not in conflict_columns
            ]
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_={col: stmt.excluded[col] for col in update_columns}
            )

        result = self._execute(stmt, table_name, operation="upsert")
        return result.rowcount if result is not None else 0

    def infer_conflict_columns(table: Table) -> List[str]:
        # Prefer primary key if defined
        if table.primary_key and table.primary_key.columns:
            return [col.name for col in table.primary_key.columns]

        # Fallback to first unique constraint
        for constraint in table.constraints:
            if getattr(constraint, "unique", False):
                return [col.name for col in constraint.columns]

        raise ValueError(f"No primary key or unique constraint found on table '{table.name}' for conflict resolution.")

    def upsert_many(
            self,
            table_name: str,
            rows: List[Dict[str, Any]],
            conflict_columns: Optional[List[str]] = None,
            conflict_constraint: Optional[str] = None,
            update_columns: Optional[List[str]] = None,
            do_nothing: bool = False
    ) -> int:
        if not rows:
            return 0

        table = self.meta.tables[table_name]
        stmt = pg_insert(table).values(rows)

        # Automatically detect conflict columns if not explicitly given
        if not conflict_columns and not conflict_constraint:
            conflict_columns = self.infer_conflict_columns(table)

        if do_nothing:
            if conflict_constraint:
                stmt = stmt.on_conflict_do_nothing(constraint=conflict_constraint)
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
        else:
            update_columns = update_columns or list(rows[0].keys())
            update_clause = {col: stmt.excluded[col] for col in update_columns if col not in (conflict_columns or [])}

            if conflict_constraint:
                stmt = stmt.on_conflict_do_update(constraint=conflict_constraint, set_=update_clause)
            else:
                stmt = stmt.on_conflict_do_update(index_elements=conflict_columns, set_=update_clause)

        result = self._execute(stmt, table_name, operation="upsert_many")
        return result.rowcount if result is not None else 0

    def update(self, table_name: str, values: Dict[str, Any], where: Dict[str, Any]) -> None:
        table = self.meta.tables[table_name]
        stmt = update(table).where(
            and_(*[table.c[k] == v for k, v in where.items()])
        ).values(**values)
        result = self._execute(stmt, table_name, "update")
        return result.rowcount if result is not None else 0

    def delete(self, table_name: str, where: Dict[str, Any]) -> None:
        table = self.meta.tables[table_name]
        stmt = delete(table).where(and_(*[table.c[k] == v for k, v in where.items()]))
        result = self._execute(stmt, table_name, "delete")
        return result.rowcount if result is not None else 0

    def health_check(self) -> bool:
        try:
            with self.engine.begin() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            logger.warning(f"[DB HEALTH CHECK FAILED] {type(e).__name__}: {e}")
            return False


if __name__ == '__main__':
    # db = RelationalDbClient(db_url="postgresql+psycopg2://postgres:@localhost/cq")
    db = RelationalDbClient(db_url="postgresql://postgres:@localhost/cq")
    print(db.health_check())
    entity = db.meta.tables["entity"]
    employee = db.meta.tables["employee"]
    department = db.meta.tables["department"]
    stmt = (
        select(
            entity.c.id,
            entity.c.name,
        ).where(or_(
            entity.c.id == 2,
            entity.c.name == 'Workforce'
        ))
    )
    print(db.execute(stmt, "entity"))

    from sqlalchemy import select, join
    employee_entity_join = join(employee, entity, employee.c.entity_id == entity.c.id)

    stmt = (
        select(
            employee.c.id.label("employee_id"),
            employee.c.name.label("employee_name"),
            entity.c.name.label("entity_name"),
        )
        .select_from(employee_entity_join)
    )
    print(db.execute(stmt, ["x","y", "x", "y"]))

    stmt = select(
        employee.c.id.label("employee_id"),
        entity.c.name.label("entity_name"),
        department.c.name.label("department_name"),
    ).select_from(
        employee
        .join(entity, employee.c.entity_id == entity.c.id)
        .join(department, employee.c.department_id == department.c.id)
    )
    print(db.execute(stmt))
    #
    # print(db.fetch_all("entity"))
    # print(db.fetch_one("entity", {'id': 2}))
    # print(db.insert("entity", {'id': 4, 'name': '3rd Party'}))

# class ReportingDbClient:
#     def __init__(self, db: RelationalDbClient):
#         self.db = db
#
#     def get_user_sales_summary(self, org_id: str):
#         users = self.db.meta.tables["users"]
#         orders = self.db.meta.tables["orders"]
#
#         stmt = (
#             select(users.c.email, func.sum(orders.c.total).label("total_spent"))
#             .select_from(users.join(orders))
#             .where(users.c.org_id == org_id)
#             .group_by(users.c.email)
#         )
#
#         return self.db.execute_statement(stmt, operation="user_sales_summary")

# | Query Type             | Supported?   | Notes                              |
# | ---------------------- | ----------   | ---------------------------------- |
# | Multi-table joins      | ✅           | Use `.join()` and `select_from()`  |
# | Aggregations/groupings | ✅          | Use `func.*` and `.group_by()`     |
# | Subqueries or CTEs     | ✅          | SQLAlchemy Core supports both      |
# | Raw SQL (fallback)     | ✅          | Use `text("...")` and `_execute()` |
# | Custom query builder   | ✅          | Define in `3rdPartyDbClient` etc.  |

# Summary: Why SQLAlchemy Core Over ORM for Enterprise-Grade Clients
# | Criteria                           | SQLAlchemy Core                                      | SQLAlchemy ORM                                   |
# | ---------------------------------- | ---------------------------------------------------- | ------------------------------------------------ |
# | 🔒 **Security (SQL Injection)**    | ✅ Safe via bound parameters                          | ✅ Also safe via bound parameters                 |
# | ⚙️ **Control over queries**        | ✅ Full control, explicit joins, expressions          | ❌ ORM auto-generates joins, eager/lazy confusion |
# | 🚀 **Performance**                 | ✅ Lean, predictable SQL                              | ❌ Often slower due to hydration + identity map   |
# | 🧱 **Composable clients**          | ✅ Easily layered in service architecture             | ⚠️ Tight coupling via model classes              |
# | 💼 **Multi-database support**      | ✅ Works with any schema, flexible metadata           | ❌ Requires mapped classes per schema             |
# | 💾 **Dynamic schema (reflection)** | ✅ Supported via `MetaData.reflect()`                 | ❌ Difficult without class generation             |
# | 🧪 **Testability**                 | ✅ Testable with mock engines or statement inspection | ❌ Harder to test due to session state            |

# 💡 Specific to Your Use Case
# You are building a general-purpose, extensible database client that:
#
# Connects to multiple RDBMS types (Postgres today, others later)
#
# Is used by higher-level clients like 3rdPartyDbClient
#
# Needs metrics, logging, and traceability
#
# Should not tie logic to domain models
#
# May be used across multiple schemas or dynamic tenants
#
# In such cases, the ORM gets in the way. It tries to map Python classes to DB rows, but that’s not how you’re architecting your system.

# from sqlalchemy import func
# class ThirdPartyDbClient:
#     def __init__(self, db: RelationalDbClient):
#         self.db = db
#
#     def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
#         return self.db.fetch_one("users", {"email": email})
#
#     def save_event(self, user_id: str, event: Dict[str, Any]) -> None:
#         self.db.insert("events", {"user_id": user_id, **event})
#     def fetch_users_with_latest_order(self) -> List[Dict[str, Any]]:
#         users = self.meta.tables["users"]
#         orders = self.meta.tables["orders"]
#
        # stmt = (
        #     select(
        #         users.c.id.label("user_id"),
        #         users.c.email,
        #         orders.c.id.label("order_id"),
        #         orders.c.total,
        #         orders.c.created_at
        #     )
#             .select_from(users.join(orders, users.c.id == orders.c.user_id))
#             .order_by(users.c.id, orders.c.created_at.desc())
#         )
#
#         return self._execute(stmt, "users_orders", "fetch_with_join")

#
# | Capability              | ✅ Provided by this version                  |
# | ----------------------- | ------------------------------------------- |
# | Safe from SQL injection | ✅ SQLAlchemy Core (parameter binding)       |
# | RDS + multi-cloud ready | ✅ Abstracted by SQLAlchemy `engine`         |
# | Metrics and logging     | ✅ Prometheus + warnings                     |
# | Reuse/composition       | ✅ Easy to wrap in business services         |
# | ORM-free performance    | ✅ Uses SQLAlchemy Core (not ORM)            |
# | Async upgrade path      | ✅ Easily upgradable to SQLAlchemy 2.0 async |

