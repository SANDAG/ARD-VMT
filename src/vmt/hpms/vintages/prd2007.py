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
    year=2007,
    path=Path("./data/hpms/raw/2007_PRD.xlsx"),
    jurisdiction_sheet_name="2007 PRD_Table 6",
    mpo_sheet_name="2007 PRD_Table 11",
    description="...",
)


def extract_2007prd_jurisdiction(
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
            rural_maintained_miles_column="D",
            urban_maintained_miles_column="E",
            total_maintained_miles_column="F",
            rural_dvmt_column="H",
            urban_dvmt_column="I",
            total_dvmt_column="J",
        ),
    )


def extract_2007prd_mpo(
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
    extract_2007prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("B", "CARLSBAD", 812),
        ("B", "CHULA_VISTA", 813),
        ("B", "CORONADO", 814),
        ("B", "DEL_MAR", 815),
        ("B", "EL_CAJON", 816),
        ("B", "ENCINITAS", 817),
        ("B", "ESCONDIDO", 818),
        ("B", "IMPERIAL_BEACH", 819),
        ("B", "LA_MESA", 820),
        ("B", "LEMON_GROVE", 821),
        ("B", "NATIONAL_CITY", 822),
        ("B", "OCEANSIDE", 823),
        ("B", "POWAY", 824),
        ("B", "SAN_DIEGO", 825),
        ("B", "SAN_MARCOS", 826),
        ("B", "SANTEE", 827),
        ("B", "SOLANA_BEACH", 828),
        ("B", "VISTA", 829),
        ("B", "US_BUREAU_OF_INDIAN_AFFAIRS", 831),
        ("B", "UNINCORPORATED", 832),
        ("B", "US_DEPARTMENT_OF_DEFENSE", 833),
        ("B", "INDIAN_TRIBAL_NATION", 834),
        ("B", "US_NATIONAL_PARK_SERVICE", 835),
        ("B", "SAN_DIEGO_UNIFIED_PORT_DISTRICT", 836),
        ("B", "STATE_HIGHWAY", 837),
        ("B", "STATE_PARKS_AND_REC", 838),
        ("B", "US_FISH_AND_WILDLIFE", 839),
        ("B", "US_FOREST_SERVICE", 840),
    ]
]


MPO_EXTRACTS = [
    extract_2007prd_mpo(
        mpo_column=col,
        mpo_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("A", "AMBAG", 7),
        ("A", "BCAG", 8),
        ("A", "SACOG", 9),
        ("A", "SANDAG", 10),
        ("A", "MCAG", 11),
        ("A", "FCOG", 12),  # COFCG IN 2007
        ("A", "SRTA", 13),  # SCRTPA IN 2007
        ("A", "KCAG", 14),
        ("A", "SJCOG", 15),
        ("A", "KCOG", 16),  # KERNCOG IN 2007
        ("A", "SLOCOG", 17),
        ("A", "MTC", 18),
        ("A", "STANCOG", 19),
        ("A", "SBCAG", 20),
        ("A", "SCAG", 21),
        ("A", "TCAG", 22),
        ("A", "TRPA", 23),  # TMPO IN 2007
        ("A", "MCTC", 24),  # MADCAG IN 2007
        ("A", "NONE", 26),
    ]
]
