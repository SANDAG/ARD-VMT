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
    year=1996,
    path=Path("./data/hpms/raw/1996PRD.xls"),
    jurisdiction_sheet_name="Sheet1",
    mpo_sheet_name=None,  # No MPO data in PRD until 2001
    description="...",
)


def extract_1996prd_jurisdiction(
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
            rural_maintained_miles_column="E",
            urban_maintained_miles_column="G",
            total_maintained_miles_column="I",
            rural_dvmt_column="M",
            urban_dvmt_column="O",
            total_dvmt_column="R",
        ),
    )


JURISDICTION_EXTRACTS = [
    extract_1996prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("C", "CARLSBAD", 842),
        ("C", "CHULA_VISTA", 843),
        ("C", "CORONADO", 844),
        ("C", "DEL_MAR", 845),
        ("C", "EL_CAJON", 846),
        ("C", "ENCINITAS", 847),
        ("C", "ESCONDIDO", 848),
        ("C", "IMPERIAL_BEACH", 849),
        ("C", "LA_MESA", 850),
        ("C", "LEMON_GROVE", 851),
        ("C", "NATIONAL_CITY", 852),
        ("C", "OCEANSIDE", 853),
        ("C", "POWAY", 854),
        ("C", "SAN_DIEGO", 855),
        ("C", "SAN_MARCOS", 856),
        ("C", "SANTEE", 857),
        ("C", "SOLANA_BEACH", 858),
        ("C", "VISTA", 859),
        ("B", "UNINCORPORATED", 861),
        ("B", "STATE_HIGHWAY", 862),
        ("B", "STATE_PARKS_AND_REC", 863),
        ("B", "US_BUREAU_OF_INDIAN_AFFAIRS", 864),
        ("B", "US_BUREAU_OF_LAND_MANAGEMENT", 865),
        ("B", "US_FOREST_SERVICE", 866),
        ("B", "US_NATIONAL_PARK_SERVICE", 874),
    ]
]
