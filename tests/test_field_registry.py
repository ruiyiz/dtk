import pytest

from dtk._field_registry import FieldRegistry
from dtk.errors import DtkFieldNotFoundError


def test_registry_empty(store):
    # No fields inserted yet; registry should be empty DataFrame
    assert store._fields.all.is_empty()


def test_get_returns_none_for_unknown(store):
    assert store._fields.get("NO_SUCH_FIELD") is None


def test_get_id_raises_for_unknown(store):
    with pytest.raises(DtkFieldNotFoundError):
        store._fields.get_id("NO_SUCH_FIELD")


def test_get_known_field(seeded_store):
    rec = seeded_store._fields.get("PX_CLOSE")
    assert rec is not None
    assert rec["DataType"] == "dbl"


def test_build_type_map(seeded_store):
    tmap = seeded_store._fields.build_type_map(["PX_CLOSE", "PX_OPEN"])
    assert tmap == {"PX_CLOSE": "dbl", "PX_OPEN": "dbl"}


def test_map_to_storage(seeded_store):
    storage = seeded_store._fields.map_to_storage(["PX_CLOSE"])
    assert "wide" in storage
    assert storage["wide"]["FieldMnemonic"][0] == "PX_CLOSE"
