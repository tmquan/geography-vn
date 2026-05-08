"""Scraper for https://sapnhap.bando.com.vn/ — Vietnam's official post-merger
administrative-units atlas published by the Ministry of Agriculture and
Environment / Vietnam Cartographic Publishing House under Resolution
202/2025/QH15 (12 June 2025).
"""

from packages.scraper.normalise import normalise_name, strip_type_prefix
from packages.scraper.sapnhap import (
    AdminUnitListing,
    CommitteeListing,
    SapnhapClient,
    parse_vi_decimal,
    parse_vi_int,
)

__all__ = [
    "SapnhapClient",
    "AdminUnitListing",
    "CommitteeListing",
    "parse_vi_decimal",
    "parse_vi_int",
    "normalise_name",
    "strip_type_prefix",
]
