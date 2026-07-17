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
    year=2009,
    path=Path("./data/hpms/raw/2009_PRD.xlsx"),
    jurisdiction_sheet_name="Table 1",
    mpo_sheet_name="Table 1",
    description="...",
)


def extract_2009prd_jurisdiction(
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
            rural_maintained_miles_column="AH",
            urban_maintained_miles_column="AR",
            total_maintained_miles_column="AY",
            rural_dvmt_column="BJ",
            urban_dvmt_column="BR",
            total_dvmt_column="BX",
        ),
    )


def extract_2009prd_mpo(
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
            total_maintained_miles_column="F",
            total_lane_miles_column="S",
            total_dvmt_column="AB",
        ),
    )


JURISDICTION_EXTRACTS = [
    extract_2009prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("E", "CARLSBAD", 926),
        ("E", "CHULA_VISTA", 927),
        ("E", "CORONADO", 928),
        ("E", "DEL_MAR", 929),
        ("E", "EL_CAJON", 930),
        ("E", "ENCINITAS", 931),
        ("E", "ESCONDIDO", 932),
        ("E", "IMPERIAL_BEACH", 933),
        ("E", "LA_MESA", 934),
        ("E", "LEMON_GROVE", 935),
        ("E", "NATIONAL_CITY", 936),
        ("E", "OCEANSIDE", 937),
        ("E", "POWAY", 938),
        ("E", "SAN_DIEGO", 939),
        ("E", "SAN_MARCOS", 940),
        ("E", "SANTEE", 941),
        ("E", "SOLANA_BEACH", 942),
        ("E", "VISTA", 943),
        #
        ("E", "US_BUREAU_OF_INDIAN_AFFAIRS", 944),
        ("E", "UNINCORPORATED", 945),
        ("E", "US_DEPARTMENT_OF_DEFENSE", 946),
        ("E", "INDIAN_TRIBAL_NATION", 947),
        ("E", "US_NATIONAL_PARK_SERVICE", 948),
        ("E", "SAN_DIEGO_UNIFIED_PORT_DISTRICT", 949),
        ("E", "STATE_HIGHWAY", 950),
        ("E", "STATE_PARKS_AND_REC", 951),
        ("E", "US_FISH_AND_WILDLIFE", 952),
        ("E", "US_FOREST_SERVICE", 953),
    ]
]


MPO_EXTRACTS = [
    extract_2009prd_mpo(
        mpo_column=col,
        mpo_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("A", "AMBAG", 1405),
        ("A", "BCAG", 1406),
        ("A", "FCOG", 1407),  # COFCG IN 2008
        ("A", "KCOG", 1408),  # KERNCOG IN 2008
        ("A", "KCAG", 1409),  # KINGKAG in 2008
        ("A", "MCAG", 1410),
        ("A", "MCTC", 1411),  # MADCAG IN 2008
        ("A", "MTC", 1412),
        ("A", "SACOG", 1413),
        ("A", "SANDAG", 1414),
        ("A", "SBCAG", 1415),
        ("A", "SCAG", 1416),
        ("A", "SRTA", 1417),  # SCRTPA IN 2008
        ("A", "SJCOG", 1418),
        ("A", "SLOCOG", 1419),
        ("A", "STANCOG", 1420),
        ("A", "TCAG", 1421),
        ("A", "TRPA", 1422),  # TMPO IN 2008
        ("A", "NONE", 1423),
    ]
]
