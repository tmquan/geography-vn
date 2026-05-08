"""Vietnamese place-name capitalisation normaliser."""

from __future__ import annotations

import pytest

from packages.scraper.normalise import normalise_name, strip_type_prefix


@pytest.mark.parametrize(
    "raw,want",
    [
        # Type-prefix canonicalisation (the main job)
        ("Thành Phố Cần Thơ",       "Thành phố Cần Thơ"),
        ("Thành phố Đồng Nai",      "Thành phố Đồng Nai"),
        ("Thủ Đô Hà Nội",           "Thủ đô Hà Nội"),
        ("Thủ đô Hà Nội",           "Thủ đô Hà Nội"),
        ("Đặc Khu Phú Quốc",        "Đặc khu Phú Quốc"),
        ("Đặc khu Phú Quốc",        "Đặc khu Phú Quốc"),
        ("Tỉnh An Giang",           "Tỉnh An Giang"),
        ("Phường Ba Đình",          "Phường Ba Đình"),
        ("Xã Hòn Nghệ",             "Xã Hòn Nghệ"),
        ("Thị Trấn Cát Bà",         "Thị trấn Cát Bà"),
        ("Ủy ban nhân dân Xã Hòn Nghệ", "Ủy ban nhân dân Xã Hòn Nghệ"),
        # Idempotence
        ("Thành phố Cần Thơ",       "Thành phố Cần Thơ"),
        # Whitespace + None
        ("  Tỉnh Cao Bằng  ",       "Tỉnh Cao Bằng"),
        ("",                         ""),
        (None,                       ""),
    ],
)
def test_normalise_name(raw, want):
    assert normalise_name(raw) == want


def test_normalise_idempotent():
    for s in ["Thành Phố Cần Thơ", "Thủ Đô Hà Nội", "Tỉnh An Giang", ""]:
        once = normalise_name(s)
        twice = normalise_name(once)
        assert once == twice


@pytest.mark.parametrize(
    "raw,want",
    [
        ("Thành Phố Cần Thơ",  "Cần Thơ"),
        ("Thủ Đô Hà Nội",      "Hà Nội"),
        ("Tỉnh An Giang",      "An Giang"),
        ("Phường Ba Đình",     "Ba Đình"),
        ("Đặc khu Phú Quốc",   "Phú Quốc"),
        ("Cần Thơ",            "Cần Thơ"),  # no prefix → passthrough
    ],
)
def test_strip_type_prefix(raw, want):
    assert strip_type_prefix(raw) == want
