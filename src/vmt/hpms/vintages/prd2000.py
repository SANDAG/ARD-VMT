from pathlib import Path

import polars as pl

from .. import hpms

JURISDICTIONS = {
    "CARLSBAD": hpms.JurisdictionInfo(name="City of Carlsbad", group="Local"),
    "CHULA_VISTA": hpms.JurisdictionInfo(name="City of Chula Vista", group="Local"),
    "CORONADO": hpms.JurisdictionInfo(name="City of Coronado", group="Local"),
    "DEL_MAR": hpms.JurisdictionInfo(name="City of Del Mar", group="Local"),
    "EL_CAJON": hpms.JurisdictionInfo(name="City of El Cajon", group="Local"),
    "ENCINITAS": hpms.JurisdictionInfo(name="City of Encinitas", group="Local"),
    "ESCONDIDO": hpms.JurisdictionInfo(name="City of Escondido", group="Local"),
    "IMPERIAL_BEACH": hpms.JurisdictionInfo(
        name="City of Imperial Beach", group="Local"
    ),
    "LA_MESA": hpms.JurisdictionInfo(name="City of La Mesa", group="Local"),
    "LEMON_GROVE": hpms.JurisdictionInfo(name="City of Lemon Grove", group="Local"),
    "NATIONAL_CITY": hpms.JurisdictionInfo(name="National City", group="Local"),
    "OCEANSIDE": hpms.JurisdictionInfo(name="City of Oceanside", group="Local"),
    "POWAY": hpms.JurisdictionInfo(name="City of Poway", group="Local"),
    "SAN_DIEGO": hpms.JurisdictionInfo(name="City of San Diego", group="Local"),
    "SAN_MARCOS": hpms.JurisdictionInfo(name="City of San Marcos", group="Local"),
    "SANTEE": hpms.JurisdictionInfo(name="City of Santee", group="Local"),
    "SOLANA_BEACH": hpms.JurisdictionInfo(name="City of Solana Beach", group="Local"),
    "VISTA": hpms.JurisdictionInfo(name="City of Vista", group="Local"),
    "UNINCORPORATED": hpms.JurisdictionInfo(name="Unincorporated", group="Local"),
    "STATE_HIGHWAY": hpms.JurisdictionInfo(name="Caltrans", group="State"),
    "STATE_PARKS_AND_REC": hpms.JurisdictionInfo(
        name="California Parks and Recreation", group="State"
    ),
    "US_BUREAU_OF_INDIAN_AFFAIRS": hpms.JurisdictionInfo(
        name="U.S. Bureau of Indian Affairs", group="Federal"
    ),
    "US_MILITARY": hpms.JurisdictionInfo(name="U.S. Military", group="Federal"),
    "US_FOREST_SERVICE": hpms.JurisdictionInfo(
        name="U.S. Forest Service", group="Federal"
    ),
    "US_NATIONAL_PARK_SERVICE": hpms.JurisdictionInfo(
        name="U.S. National Park Service", group="Federal"
    ),
}


SOURCE = hpms.PublicRoadDataExcelSource(
    year=2000,
    path=Path("./data/hpms/raw/2000PRD.xls"),
    jurisdiction_sheet_name="Table 2-1-6",
    mpo_sheet_name=None,  # No MPO data in PRD until 2001
    description="...",
)


def extract_2000prd_jurisdiction(
    jurisdiction_column: str,
    jurisdiction_key: str,
    row_number: int,
) -> pl.DataFrame:
    owner_info = JURISDICTIONS[jurisdiction_key]
    return SOURCE.extract_row(
        owner_info=owner_info,
        row_info=hpms.RowInfo(
            row_number=row_number,
            jurisdiction_column=jurisdiction_column,
            rural_maintained_miles_column="G",
            urban_maintained_miles_column="I",
            total_maintained_miles_column="K",
            rural_dvmt_column="O",
            urban_dvmt_column="Q",
            total_dvmt_column="S",
        ),
    )


JURISDICTION_EXTRACTS = [
    extract_2000prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("E", "CARLSBAD", 693),
        ("E", "CHULA_VISTA", 694),
        ("E", "CORONADO", 695),
        ("E", "DEL_MAR", 696),
        ("E", "EL_CAJON", 697),
        ("E", "ENCINITAS", 698),
        ("E", "ESCONDIDO", 699),
        ("E", "IMPERIAL_BEACH", 700),
        ("E", "LA_MESA", 701),
        ("E", "LEMON_GROVE", 702),
        ("E", "NATIONAL_CITY", 703),
        ("E", "OCEANSIDE", 704),
        ("E", "POWAY", 705),
        ("E", "SAN_DIEGO", 706),
        ("E", "SAN_MARCOS", 707),
        ("E", "SANTEE", 708),
        ("E", "SOLANA_BEACH", 709),
        ("E", "VISTA", 710),
        ("D", "UNINCORPORATED", 712),
        ("D", "STATE_HIGHWAY", 713),
        ("D", "STATE_PARKS_AND_REC", 714),
        ("D", "US_BUREAU_OF_INDIAN_AFFAIRS", 715),
        ("D", "US_FOREST_SERVICE", 716),
        ("D", "US_MILITARY", 717),
        ("D", "US_NATIONAL_PARK_SERVICE", 718),
    ]
]
