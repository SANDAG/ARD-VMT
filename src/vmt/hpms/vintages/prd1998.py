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
    "US_BUREAU_OF_LAND_MANAGEMENT": hpms.JurisdictionInfo(
        name="U.S. Bureau of Land Management", group="Federal"
    ),
    "US_FOREST_SERVICE": hpms.JurisdictionInfo(
        name="U.S. Forest Service", group="Federal"
    ),
    "US_NATIONAL_PARK_SERVICE": hpms.JurisdictionInfo(
        name="U.S. National Park Service", group="Federal"
    ),
}


SOURCE = hpms.PublicRoadDataExcelSource(
    year=1998,
    path=Path("./data/hpms/raw/1998PRD.xls"),
    jurisdiction_sheet_name="Table 2-1-6",
    mpo_sheet_name=None,  # No MPO data in PRD until 2001
    description="...",
)


def extract_1998prd_jurisdiction(
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
    extract_1998prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("E", "CARLSBAD", 720),
        ("E", "CHULA_VISTA", 721),
        ("E", "CORONADO", 722),
        ("E", "DEL_MAR", 723),
        ("E", "EL_CAJON", 724),
        ("E", "ENCINITAS", 725),
        ("E", "ESCONDIDO", 726),
        ("E", "IMPERIAL_BEACH", 727),
        ("E", "LA_MESA", 728),
        ("E", "LEMON_GROVE", 729),
        ("E", "NATIONAL_CITY", 730),
        ("E", "OCEANSIDE", 731),
        ("E", "POWAY", 732),
        ("E", "SAN_DIEGO", 733),
        ("E", "SAN_MARCOS", 734),
        ("E", "SANTEE", 735),
        ("E", "SOLANA_BEACH", 736),
        ("E", "VISTA", 737),
        ("D", "UNINCORPORATED", 739),
        ("D", "STATE_HIGHWAY", 740),
        ("D", "STATE_PARKS_AND_REC", 741),
        ("D", "US_BUREAU_OF_INDIAN_AFFAIRS", 742),
        ("D", "US_BUREAU_OF_LAND_MANAGEMENT", 743),
        ("D", "US_FOREST_SERVICE", 744),
        ("D", "US_NATIONAL_PARK_SERVICE", 745),
    ]
]
