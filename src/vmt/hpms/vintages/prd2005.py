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

SOURCE = hpms.PublicRoadDataExcelSource(
    year=2005,
    path=Path("./data/hpms/raw/2005_PRD.xlsx"),
    jurisdiction_sheet_name="2005 PRD_Table 6",
    mpo_sheet_name="2005 PRD_Table 11",
    description="...",
)


def extract_2005prd_jurisdiction(
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
            urban_maintained_miles_column="F",
            total_maintained_miles_column="G",
            rural_dvmt_column="I",
            urban_dvmt_column="J",
            total_dvmt_column="K",
        ),
    )


def extract_2005prd_mpo(
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
            total_maintained_miles_column="D",
            total_lane_miles_column="E",
            total_dvmt_column="F",
        ),
    )


JURISDICTION_EXTRACTS = [
    extract_2005prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("B", "CARLSBAD", 843),
        ("B", "CHULA_VISTA", 844),
        ("B", "CORONADO", 845),
        ("B", "DEL_MAR", 846),
        ("B", "EL_CAJON", 847),
        ("B", "ENCINITAS", 848),
        ("B", "ESCONDIDO", 849),
        ("B", "IMPERIAL_BEACH", 850),
        ("B", "LA_MESA", 851),
        ("B", "LEMON_GROVE", 852),
        ("B", "NATIONAL_CITY", 853),
        ("B", "OCEANSIDE", 854),
        ("B", "POWAY", 855),
        ("B", "SAN_DIEGO", 856),
        ("B", "SAN_MARCOS", 857),
        ("B", "SANTEE", 858),
        ("B", "SOLANA_BEACH", 859),
        ("B", "VISTA", 860),
        ("B", "US_BUREAU_OF_INDIAN_AFFAIRS", 862),
        ("B", "UNINCORPORATED", 863),
        ("B", "US_DEPARTMENT_OF_DEFENSE", 864),
        ("B", "INDIAN_TRIBAL_NATION", 865),
        ("B", "US_NATIONAL_PARK_SERVICE", 866),
        ("B", "SAN_DIEGO_UNIFIED_PORT_DISTRICT", 867),
        ("B", "STATE_HIGHWAY", 868),
        ("B", "STATE_PARKS_AND_REC", 869),
        ("B", "US_FISH_AND_WILDLIFE", 870),
        ("B", "US_FOREST_SERVICE", 871),
    ]
]


MPO_EXTRACTS = [
    extract_2005prd_mpo(
        mpo_column=col,
        mpo_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("A", "AMBAG", 8),
        ("A", "BCAG", 9),
        ("A", "SACOG", 10),
        ("A", "SANDAG", 11),
        ("A", "MCAG", 12),
        ("A", "FCOG", 13),  # COFCG IN 2005
        ("A", "SRTA", 14),  # SCRTPA IN 2005
        ("A", "KCAG", 15),
        ("A", "SJCOG", 16),
        ("A", "KCOG", 17),  # KERNCOG IN 2005
        ("A", "SLOCOG", 18),
        ("A", "MTC", 19),
        ("A", "STANCOG", 20),
        ("A", "SBCAG", 21),
        ("A", "SCAG", 22),
        ("A", "TCAG", 23),
        ("A", "TRPA", 24),  # TMPO IN 2005
        ("A", "MCTC", 25),  # MADCAG IN 2005
        ("A", "NONE", 27),
    ]
]
