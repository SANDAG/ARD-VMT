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
    year=2008,
    path=Path("./data/hpms/raw/2008_PRD.xlsx"),
    jurisdiction_sheet_name="Table 1",
    mpo_sheet_name="Table 1",
    description="...",
)


def extract_2008prd_jurisdiction(
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
            rural_maintained_miles_column="AM",
            urban_maintained_miles_column="AV",
            total_maintained_miles_column="BD",
            rural_dvmt_column="BT",
            urban_dvmt_column="CA",
            total_dvmt_column="CI",
        ),
    )


def extract_2008prd_mpo(
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
            total_maintained_miles_column="J",
            total_lane_miles_column="U",
            total_dvmt_column="AI",
        ),
    )


JURISDICTION_EXTRACTS = [
    extract_2008prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("D", "CARLSBAD", 899),
        ("D", "CHULA_VISTA", 900),
        ("D", "CORONADO", 901),
        ("D", "DEL_MAR", 902),
        ("D", "EL_CAJON", 903),
        ("D", "ENCINITAS", 904),
        ("D", "ESCONDIDO", 905),
        ("D", "IMPERIAL_BEACH", 906),
        ("D", "LA_MESA", 907),
        ("D", "LEMON_GROVE", 908),
        ("D", "NATIONAL_CITY", 920),
        ("D", "OCEANSIDE", 909),
        ("D", "POWAY", 910),
        ("D", "SAN_DIEGO", 911),
        ("D", "SAN_MARCOS", 912),
        ("D", "SANTEE", 913),
        ("D", "SOLANA_BEACH", 914),
        ("D", "VISTA", 915),
        #
        ("D", "US_BUREAU_OF_INDIAN_AFFAIRS", 916),
        ("D", "UNINCORPORATED", 917),
        ("D", "US_DEPARTMENT_OF_DEFENSE", 918),
        ("D", "INDIAN_TRIBAL_NATION", 919),
        ("D", "US_NATIONAL_PARK_SERVICE", 921),
        ("D", "SAN_DIEGO_UNIFIED_PORT_DISTRICT", 922),
        ("D", "STATE_HIGHWAY", 923),
        ("D", "STATE_PARKS_AND_REC", 924),
        ("D", "US_FISH_AND_WILDLIFE", 925),
        ("D", "US_FOREST_SERVICE", 926),
    ]
]


MPO_EXTRACTS = [
    extract_2008prd_mpo(
        mpo_column=col,
        mpo_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("A", "AMBAG", 1393),
        ("A", "BCAG", 1394),
        ("A", "FCOG", 1395),  # COFCG IN 2008
        ("A", "KCOG", 1396),  # KERNCOG IN 2008
        ("A", "KCAG", 1397),  # KINGKAG in 2008
        ("A", "MCAG", 1398),
        ("A", "MCTC", 1399),  # MADCAG IN 2008
        ("A", "MTC", 1400),
        ("A", "SACOG", 1401),
        ("A", "SANDAG", 1402),
        ("A", "SBCAG", 1403),
        ("A", "SCAG", 1404),
        ("A", "SRTA", 1405),  # SCRTPA IN 2008
        ("A", "SJCOG", 1406),
        ("A", "SLOCOG", 1407),
        ("A", "STANCOG", 1408),
        ("A", "TCAG", 1409),
        ("A", "TRPA", 1410),  # TMPO IN 2008
        ("A", "NONE", 1411),
    ]
]
