from server.compiler.dialect_profiles import get_dialect_profile


def test_mariadb_profile_maps_to_mysql_sqlglot():
    profile = get_dialect_profile("mariadb")

    assert profile.db_type == "mariadb"
    assert profile.compiler_dialect == "mysql"
    assert profile.sqlglot_dialect == "mysql"
    assert profile.is_mysql_family is True
    assert profile.supports_full_outer_join is False


def test_sqlserver_profile_keeps_full_outer_join():
    profile = get_dialect_profile("sqlserver")

    assert profile.db_type == "sqlserver"
    assert profile.compiler_dialect == "tsql"
    assert profile.supports_full_outer_join is True
