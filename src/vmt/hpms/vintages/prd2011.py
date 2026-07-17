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
    year=2011,
    path=Path("./data/hpms/raw/2011_PRD.xlsx"),
    jurisdiction_sheet_name="2011 PRD_Table 6",
    mpo_sheet_name="2011 PRD_Table 10",
    description="...",
)


def extract_2011prd_jurisdiction(
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
            rural_maintained_miles_column="C",
            urban_maintained_miles_column="D",
            total_maintained_miles_column="E",
            rural_dvmt_column="G",
            urban_dvmt_column="H",
            total_dvmt_column="I",
        ),
    )


def extract_2011prd_mpo(
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
            total_maintained_miles_column="C",
            total_lane_miles_column="D",
            total_dvmt_column="E",
        ),
    )


JURISDICTION_EXTRACTS = [
    extract_2011prd_jurisdiction(
        jurisdiction_column=col,
        jurisdiction_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("B", "CARLSBAD", 706),
        ("B", "CHULA_VISTA", 707),
        ("B", "CORONADO", 708),
        ("B", "DEL_MAR", 709),
        ("B", "EL_CAJON", 710),
        ("B", "ENCINITAS", 711),
        ("B", "ESCONDIDO", 712),
        ("B", "IMPERIAL_BEACH", 713),
        ("B", "LA_MESA", 714),
        ("B", "LEMON_GROVE", 715),
        ("B", "NATIONAL_CITY", 716),
        ("B", "OCEANSIDE", 717),
        ("B", "POWAY", 718),
        ("B", "SAN_DIEGO", 719),
        ("B", "SAN_MARCOS", 720),
        ("B", "SANTEE", 721),
        ("B", "SOLANA_BEACH", 722),
        ("B", "VISTA", 723),
        #
        ("B", "US_BUREAU_OF_INDIAN_AFFAIRS", 725),
        ("B", "UNINCORPORATED", 726),
        ("B", "INDIAN_TRIBAL_NATION", 727),
        ("B", "US_NATIONAL_PARK_SERVICE", 728),
        ("B", "SAN_DIEGO_UNIFIED_PORT_AUTHORITY", 729),
        ("B", "SAN_DIEGO_UNIFIED_PORT_DISTRICT", 730),
        ("B", "STATE_HIGHWAY", 731),
        ("B", "STATE_PARKS_AND_REC", 732),
        ("B", "US_ARMY", 733),
        ("B", "US_MARINE_CORPS", 734),
        ("B", "US_NAVY", 735),
        ("B", "US_FISH_AND_WILDLIFE", 736),
        ("B", "US_FOREST_SERVICE", 737),
    ]
]


MPO_EXTRACTS = [
    extract_2011prd_mpo(
        mpo_column=col,
        mpo_key=key,
        row_number=number,
    )
    for (col, key, number) in [
        ("B", "AMBAG", 7),
        ("B", "BCAG", 8),
        ("B", "FCOG", 9),  # COFCG IN 2011
        ("B", "KCOG", 10),  # KERNCOG IN 2011
        ("B", "KCAG", 11),  # KINGCAG in 2011
        ("B", "MCAG", 12),
        ("B", "MCTC", 13),  # MADCAG IN 2011
        ("B", "MTC", 14),
        ("B", "SACOG", 15),
        ("B", "SANDAG", 16),
        ("B", "SBCAG", 17),
        ("B", "SCAG", 18),
        ("B", "SRTA", 19),  # SCRTPA IN 2011
        ("B", "SJCOG", 20),
        ("B", "SLOCOG", 21),
        ("B", "STANCOG", 22),
        ("B", "TCAG", 23),
        ("B", "TRPA", 24),  # TMPO IN 2011
        ("B", "NONE", 26),
    ]
]
