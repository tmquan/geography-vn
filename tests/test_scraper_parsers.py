"""Vietnamese / English number parsers used by the scraper."""

from __future__ import annotations

import pytest

from packages.scraper.sapnhap import (
    AdminUnitListing,
    CommitteeListing,
    parse_vi_decimal,
    parse_vi_int,
)


@pytest.mark.parametrize(
    "value,want",
    [
        ("6.360,83",  6360.83),  # vi: dot=thousand, comma=decimal
        ("4.199.824", 4199824.0),  # vi: multiple dots = thousands
        ("2,97",      2.97),      # vi short
        ("575.29",    575.29),    # en: single dot = decimal
        ("65.023",    65.023),    # ambiguous → English wins (single dot)
        ("",          None),
        (None,        None),
        ("đang cập nhật", None),
        ("n/a",       None),
        ("Null",      None),
    ],
)
def test_parse_vi_decimal(value, want):
    assert parse_vi_decimal(value) == want


@pytest.mark.parametrize(
    "value,want",
    [
        ("4.199.824", 4199824),   # vi
        ("65.023",    65023),     # vi (population is integer-only)
        ("157629",    157629),    # bare int
        ("",          None),
        (None,        None),
        ("đang cập nhật", None),
    ],
)
def test_parse_vi_int(value, want):
    assert parse_vi_int(value) == want


def test_admin_unit_listing_level_classification():
    province = AdminUnitListing.from_dict({
        "ma": "92", "ten": "Thành Phố Cần Thơ", "magoc": "0",
        "malk": "diaphanhanhchinhcaptinh_sn.1",
        "truocsapnhap": "thành phố Cần Thơ, tỉnh Sóc Trăng và tỉnh Hậu Giang",
    })
    assert province.level == "province"
    assert province.ten == "Thành Phố Cần Thơ"

    commune = AdminUnitListing.from_dict({
        "ma": "00004", "ten": "Phường Ba Đình", "magoc": "01",
        "malk": "diaphanhanhchinhcapxa_2025.3256",
        "truocsapnhap": "Phường Quán Thánh, Phường Trúc Bạch",
    })
    assert commune.level == "commune"


def test_committee_listing_from_dict():
    c = CommitteeListing.from_dict({"id": 1, "ten": "Đặc khu Phú Quốc",
                                    "ma": "uybannhandancapxa_2025.1"})
    assert c.id == 1
    assert c.ma == "uybannhandancapxa_2025.1"
