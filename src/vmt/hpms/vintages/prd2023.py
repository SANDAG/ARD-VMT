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
    "OTHER_STATE_AGENCIES": hpms.JurisdictionInfo(
        name="Other State Agencies", group="State"
    ),
    "US_BUREAU_OF_INDIAN_AFFAIRS": hpms.JurisdictionInfo(
        name="U.S. Bureau of Indian Affairs", group="Federal"
    ),
    "US_DEPARTMENT_OF_DEFENSE": hpms.JurisdictionInfo(
        name="U.S. Department of Defense", group="Federal"
    ),
    "US_MILITARY": hpms.JurisdictionInfo(name="U.S. Military", group="Federal"),
    "US_FOREST_SERVICE": hpms.JurisdictionInfo(
        name="U.S. Forest Service", group="Federal"
    ),
    "US_NATIONAL_PARK_SERVICE": hpms.JurisdictionInfo(
        name="U.S. National Park Service", group="Federal"
    ),
    "US_FISH_AND_WILDLIFE": hpms.JurisdictionInfo(
        name="U.S. Fish and Wildlife Service",
        group="Federal",
    ),
    "SAN_DIEGO_UNIFIED_PORT_AUTHORITY": hpms.JurisdictionInfo(
        name="San Diego Unified Port Authority",
        group="Other",
    ),
    "SAN_DIEGO_UNIFIED_PORT_DISTRICT": hpms.JurisdictionInfo(
        name="San Diego Unified Port District",
        group="Other",
    ),
    "INDIAN_TRIBAL_NATION": hpms.JurisdictionInfo(
        name="Indian Tribal Nation",
        group="Other",
    ),
    "OFA": hpms.JurisdictionInfo(
        name="OFA",
        group="Other",
    ),
    "US_ARMY": hpms.JurisdictionInfo(
        name="U.S. Army",
        group="Federal",
    ),
    "US_MARINE_CORPS": hpms.JurisdictionInfo(
        name="U.S. Marine Corps",
        group="Federal",
    ),
    "US_ARMY_AND_MARINE_CORPS": hpms.JurisdictionInfo(
        name="U.S. Army/Marine Corps",
        group="Federal",
    ),
    "US_NAVY": hpms.JurisdictionInfo(
        name="U.S. Navy",
        group="Federal",
    ),
    "US_BUREAU_OF_LAND_MANAGEMENT": hpms.JurisdictionInfo(
        name="U.S. Bureau of Land Management",
        group="Federal",
    ),
}


MPOS = {
    "AMBAG": hpms.MPOInfo(
        name="Association of Monterey Bay Area Governments", abbreviation="AMBAG"
    ),
    "BCAG": hpms.MPOInfo(
        name="Butte County Association of Governments", abbreviation="BCAG"
    ),
    "FCOG": hpms.MPOInfo(name="Fresno Council of Governments", abbreviation="FCOG"),
    "KCAG": hpms.MPOInfo(
        name="Kings County Association of Governments", abbreviation="KCAG"
    ),
    "KCOG": hpms.MPOInfo(name="Kern Council of Governments", abbreviation="KCOG"),
    "MCAG": hpms.MPOInfo(
        name="Merced County Association of Governments", abbreviation="MCAG"
    ),
    "MCTC": hpms.MPOInfo(
        name="Madera County Transportation Commission", abbreviation="MCTC"
    ),
    "MTC": hpms.MPOInfo(
        name="Metropolitan Transportation Commission", abbreviation="MTC"
    ),
    "SACOG": hpms.MPOInfo(
        name="Sacramento Area Council of Governments", abbreviation="SACOG"
    ),
    "SANDAG": hpms.MPOInfo(
        name="San Diego Association of Governments", abbreviation="SANDAG"
    ),
    "SBCAG": hpms.MPOInfo(
        name="Santa Barbara County Association of Governments", abbreviation="SBCAG"
    ),
    "SCAG": hpms.MPOInfo(
        name="Southern California Association of Governments", abbreviation="SCAG"
    ),
    "SJCOG": hpms.MPOInfo(
        name="San Joaquin Council of Governments", abbreviation="SJCOG"
    ),
    "SLOCOG": hpms.MPOInfo(
        name="San Luis Obispo Council of Governments", abbreviation="SLOCOG"
    ),
    "SRTA": hpms.MPOInfo(
        name="Shasta Regional Transportation Agency", abbreviation="SRTA"
    ),
    "STANCOG": hpms.MPOInfo(
        name="Stanislaus Council of Governments", abbreviation="StanCOG"
    ),
    "TCAG": hpms.MPOInfo(
        name="Tulare County Association of Governments", abbreviation="TCAG"
    ),
    "TRPA": hpms.MPOInfo(name="Tahoe Regional Planning Agency", abbreviation="TRPA"),
    "NONE": hpms.MPOInfo(name="Not in any MPO", abbreviation="None"),
}


SOURCE = hpms.PublicRoadDataPDFSource(
    year=2023,
    pdf_path=Path("./data/hpms/raw/hpms2023-prd-final.pdf"),
    pdf_jurisdiction_page=94,
    pdf_mpo_page=142,
    extract_path=Path("./data/hpms/raw/2023 HPMS Extract.xlsx"),
    extract_jurisdiction_sheet_name="Jurisdiction",
    extract_mpo_sheet_name="MPO",
    description="...",
)


def extract_2023prd_jurisdiction(
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
            rural_maintained_miles_column="B",
            urban_maintained_miles_column="C",
            total_maintained_miles_column="D",
            rural_dvmt_column="E",
            urban_dvmt_column="F",
            total_dvmt_column="G",
        ),
    )


def extract_2023prd_mpo(
    mpo_column: str,
    mpo_key: str,
    row_number: int,
) -> pl.DataFrame:
    owner_info = MPOS[mpo_key]
    return SOURCE.extract_row(
        owner_info=owner_info,
        row_info=hpms.RowInfo(
            row_number=row_number,
            mpo_column=mpo_column,
            total_maintained_miles_column="B",
            total_lane_miles_column="C",
            total_dvmt_column="D",
        ),
    )


JURISDICTION_EXTRACTS = [
    extract_2023prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("A", "CARLSBAD", 3),
        ("A", "CHULA_VISTA", 4),
        ("A", "CORONADO", 5),
        ("A", "DEL_MAR", 6),
        ("A", "EL_CAJON", 7),
        ("A", "ENCINITAS", 8),
        ("A", "ESCONDIDO", 9),
        ("A", "IMPERIAL_BEACH", 10),
        ("A", "LA_MESA", 11),
        ("A", "LEMON_GROVE", 12),
        ("A", "NATIONAL_CITY", 13),
        ("A", "OCEANSIDE", 14),
        ("A", "POWAY", 15),
        ("A", "SAN_DIEGO", 16),
        ("A", "SAN_MARCOS", 17),
        ("A", "SANTEE", 18),
        ("A", "SOLANA_BEACH", 19),
        ("A", "VISTA", 20),
        #
        ("A", "US_BUREAU_OF_INDIAN_AFFAIRS", 21),
        ("A", "US_NATIONAL_PARK_SERVICE", 22),
        ("A", "OTHER_STATE_AGENCIES", 23),
        ("A", "UNINCORPORATED", 24),
        ("A", "STATE_HIGHWAY", 25),
        ("A", "STATE_PARKS_AND_REC", 26),
        ("A", "US_BUREAU_OF_LAND_MANAGEMENT", 27),
        ("A", "US_FISH_AND_WILDLIFE", 28),
        ("A", "US_FOREST_SERVICE", 29),
    ]
]


MPO_EXTRACTS = [
    extract_2023prd_mpo(
        mpo_column=col,
        mpo_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("A", "AMBAG", 2),
        ("A", "BCAG", 3),
        ("A", "FCOG", 4),
        ("A", "KCAG", 5),
        ("A", "KCOG", 6),
        ("A", "MCAG", 7),
        ("A", "MCTC", 8),
        ("A", "MTC", 9),
        ("A", "SACOG", 10),
        ("A", "SANDAG", 11),
        ("A", "SBCAG", 12),
        ("A", "SCAG", 13),
        ("A", "SJCOG", 14),
        ("A", "SLOCOG", 15),
        ("A", "SRTA", 16),
        ("A", "STANCOG", 17),
        ("A", "TCAG", 18),
        ("A", "TRPA", 19),
        ("A", "NONE", 20),
    ]
]
