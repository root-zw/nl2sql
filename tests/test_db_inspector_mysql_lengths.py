from server.utils.db_inspector import normalize_mysql_max_length


def test_mysql_unbounded_types_are_normalized_to_none():
    assert normalize_mysql_max_length("longtext", 4294967295) is None
    assert normalize_mysql_max_length("json", 4294967295) is None


def test_mysql_bounded_lengths_are_preserved():
    assert normalize_mysql_max_length("varchar", 255) == 255
    assert normalize_mysql_max_length("char", "32") == 32


def test_mysql_invalid_lengths_return_none():
    assert normalize_mysql_max_length("varchar", -1) is None
    assert normalize_mysql_max_length("varchar", "not-a-number") is None
