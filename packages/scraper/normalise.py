"""Vietnamese place-name normalisation.

The ``sapnhap.bando.com.vn`` API and the underlying QGIS Server feature
properties return type-prefix capitalisation inconsistently:

* ``"Thành Phố Cần Thơ"`` (PascalCase ``Phố``)
* ``"Thành phố Đồng Nai"`` (lowercase ``phố``)
* ``"Thành phố Hồ Chí Minh"`` (different table again)
* ``"Thủ Đô Hà Nội"`` (PascalCase ``Đô``)
* ``"Thủ đô Hà Nội"`` (lowercase ``đô``, used in committee parents)

Standard Vietnamese orthography capitalises only the **first** word of a
phrase plus proper nouns. This module canonicalises every common type
prefix to that form so cross-table joins work and the rendered figures
read consistently.
"""

from __future__ import annotations

import re
import unicodedata

# (raw → canonical) mapping for the type-prefix portion of admin-unit
# names. Keys are MATCH-PATTERNS (regex, leading anchor); values are the
# canonical Vietnamese form. Order matters — longer prefixes first.
_TYPE_PREFIX_RULES: tuple[tuple[str, str], ...] = (
    (r"^Ủy ban nhân dân ",   "Ủy ban nhân dân "),
    (r"^Thủ Đô ",            "Thủ đô "),
    (r"^Thủ đô ",            "Thủ đô "),
    (r"^Thành Phố ",         "Thành phố "),
    (r"^Thành phố ",         "Thành phố "),
    (r"^Đặc Khu ",           "Đặc khu "),
    (r"^Đặc khu ",           "Đặc khu "),
    (r"^Tỉnh ",              "Tỉnh "),
    (r"^Phường ",            "Phường "),
    (r"^Xã ",                "Xã "),
    (r"^Thị Trấn ",          "Thị trấn "),
    (r"^Thị trấn ",          "Thị trấn "),
)

_TYPE_PREFIX_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = tuple(
    (re.compile(p), repl) for p, repl in _TYPE_PREFIX_RULES
)


def normalise_name(name: str | None) -> str:
    """Return ``name`` NFC-normalised + with the type prefix re-cased.

    Idempotent: running it twice returns the same value as running it once.
    """
    if name is None:
        return ""
    s = unicodedata.normalize("NFC", str(name)).strip()
    for pat, repl in _TYPE_PREFIX_PATTERNS:
        if pat.match(s):
            return pat.sub(repl, s, count=1)
    return s


# Reverse direction is sometimes useful (e.g. for filename-safe slug):
# strip the type prefix and return the bare name.
_PREFIX_STRIP_RE = re.compile(
    r"^(?:Ủy ban nhân dân |Thủ đô |Thành phố |Đặc khu |Tỉnh |Phường |Xã |Thị trấn )",
    flags=re.IGNORECASE,
)


def strip_type_prefix(name: str | None) -> str:
    """Return ``name`` with any normalised type prefix removed."""
    if name is None:
        return ""
    s = normalise_name(name)
    return _PREFIX_STRIP_RE.sub("", s).strip()


__all__ = ["normalise_name", "strip_type_prefix"]
