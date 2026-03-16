"""Dialect capability profiles for compiler and runtime decisions."""

from dataclasses import dataclass


@dataclass(frozen=True)
class DialectProfile:
    """Normalized dialect metadata and capability switches."""

    db_type: str
    compiler_dialect: str
    sqlglot_dialect: str
    display_name: str
    family: str
    supports_full_outer_join: bool
    supports_grouping_function: bool
    supports_rollup: bool
    supports_read_only_transaction: bool
    supports_statement_timeout: bool
    supports_cte: bool
    supports_window_features: bool

    @property
    def is_mysql_family(self) -> bool:
        return self.family == "mysql"


_ALIASES = {
    "mssql": "sqlserver",
    "tsql": "sqlserver",
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "sqlserver": "sqlserver",
}


_PROFILES = {
    "sqlserver": DialectProfile(
        db_type="sqlserver",
        compiler_dialect="tsql",
        sqlglot_dialect="tsql",
        display_name="SQL Server (T-SQL)",
        family="sqlserver",
        supports_full_outer_join=True,
        supports_grouping_function=True,
        supports_rollup=True,
        supports_read_only_transaction=False,
        supports_statement_timeout=True,
        supports_cte=True,
        supports_window_features=True,
    ),
    "postgresql": DialectProfile(
        db_type="postgresql",
        compiler_dialect="postgres",
        sqlglot_dialect="postgres",
        display_name="PostgreSQL",
        family="postgresql",
        supports_full_outer_join=True,
        supports_grouping_function=True,
        supports_rollup=True,
        supports_read_only_transaction=True,
        supports_statement_timeout=True,
        supports_cte=True,
        supports_window_features=True,
    ),
    "mysql": DialectProfile(
        db_type="mysql",
        compiler_dialect="mysql",
        sqlglot_dialect="mysql",
        display_name="MySQL",
        family="mysql",
        supports_full_outer_join=False,
        supports_grouping_function=False,
        supports_rollup=True,
        supports_read_only_transaction=True,
        supports_statement_timeout=True,
        supports_cte=True,
        supports_window_features=True,
    ),
    "mariadb": DialectProfile(
        db_type="mariadb",
        compiler_dialect="mysql",
        sqlglot_dialect="mysql",
        display_name="MariaDB",
        family="mysql",
        supports_full_outer_join=False,
        supports_grouping_function=False,
        supports_rollup=True,
        supports_read_only_transaction=True,
        supports_statement_timeout=True,
        supports_cte=True,
        supports_window_features=True,
    ),
}


def normalize_db_type(value: str | None) -> str:
    """Normalize db_type aliases to canonical values."""

    if not value:
        return "sqlserver"
    return _ALIASES.get(value.lower(), value.lower())


def get_dialect_profile(value: str | None) -> DialectProfile:
    """Resolve a normalized capability profile from db_type or dialect."""

    normalized = normalize_db_type(value)
    return _PROFILES.get(normalized, _PROFILES["sqlserver"])
