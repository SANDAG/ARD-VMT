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

SOURCE = hpms.PublicRoadDataExcelSource(
    year=2014,
    path=Path("./data/hpms/raw/2014_PRD.xlsx"),
    jurisdiction_sheet_name="2014 PRD_Table 6",
    mpo_sheet_name="2014 PRD_Table 9",
    description="...",
)


def extract_2014prd_jurisdiction(
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


def extract_2014prd_mpo(
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
    extract_2014prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("A", "CARLSBAD", 680),
        ("A", "CHULA_VISTA", 681),
        ("A", "CORONADO", 682),
        ("A", "DEL_MAR", 683),
        ("A", "EL_CAJON", 684),
        ("A", "ENCINITAS", 685),
        ("A", "ESCONDIDO", 686),
        ("A", "IMPERIAL_BEACH", 687),
        ("A", "LA_MESA", 688),
        ("A", "LEMON_GROVE", 689),
        ("A", "NATIONAL_CITY", 690),
        ("A", "OCEANSIDE", 691),
        ("A", "POWAY", 692),
        ("A", "SAN_DIEGO", 693),
        ("A", "SAN_MARCOS", 694),
        ("A", "SANTEE", 695),
        ("A", "SOLANA_BEACH", 696),
        ("A", "VISTA", 697),
        #
        ("A", "US_BUREAU_OF_INDIAN_AFFAIRS", 699),
        ("A", "INDIAN_TRIBAL_NATION", 700),
        ("A", "US_NATIONAL_PARK_SERVICE", 701),
        ("A", "UNINCORPORATED", 702),
        ("A", "SAN_DIEGO_UNIFIED_PORT_DISTRICT", 703),
        ("A", "STATE_HIGHWAY", 704),
        ("A", "STATE_PARKS_AND_REC", 705),
        ("A", "US_BUREAU_OF_LAND_MANAGEMENT", 706),
        ("A", "US_FOREST_SERVICE", 707),
        ("A", "US_MARINE_CORPS", 708),
        ("A", "US_NAVY", 709),
        ("A", "US_FISH_AND_WILDLIFE", 710),
    ]
]


MPO_EXTRACTS = [
    extract_2014prd_mpo(
        mpo_column=col,
        mpo_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("A", "AMBAG", 7),
        ("A", "BCAG", 8),
        ("A", "FCOG", 9),  # COFCG in 2014
        ("A", "KCOG", 10),  # KERNCOG in 2014
        ("A", "KCAG", 11),  # KINGCAG in 2014
        ("A", "MCAG", 12),
        ("A", "MCTC", 13),
        ("A", "MTC", 14),
        ("A", "SACOG", 15),
        ("A", "SANDAG", 16),
        ("A", "SBCAG", 17),
        ("A", "SCAG", 18),
        ("A", "SRTA", 19),  # SCRTPA in 2014
        ("A", "SJCOG", 20),
        ("A", "SLOCOG", 21),
        ("A", "STANCOG", 22),
        ("A", "TCAG", 23),
        ("A", "TRPA", 24),  # TMPO IN 2014
        ("A", "NONE", 25),
    ]
]
