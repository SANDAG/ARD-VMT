from pathlib import Path
from typing import Any, Literal

import polars as pl
import pydantic


class JurisdictionInfo(pydantic.BaseModel):
    name: str
    group: Literal["Local", "State", "Federal", "Other"]


class MPOInfo(pydantic.BaseModel):
    name: str
    abbreviation: str


class RowInfo(pydantic.BaseModel):
    row_number: int
    jurisdiction_column: str | None = None
    mpo_column: str | None = None
    urban_maintained_miles_column: str | None = None
    urban_dvmt_column: str | None = None
    rural_maintained_miles_column: str | None = None
    rural_dvmt_column: str | None = None
    total_maintained_miles_column: str | None = None
    total_dvmt_column: str | None = None
    total_lane_miles_column: str | None = None


class PublicRoadDataExcelSource(pydantic.BaseModel):
    year: int
    path: Path
    description: str
    jurisdiction_sheet_name: str | None = None
    mpo_sheet_name: str | None = None

    def extract_row(
        self,
        owner_info: JurisdictionInfo | MPOInfo,
        row_info: RowInfo,
    ) -> pl.DataFrame:
        match owner_info:
            case JurisdictionInfo():
                columns = {
                    "jurisdiction": row_info.jurisdiction_column,
                    "rural_maintained_miles": row_info.rural_maintained_miles_column,
                    "urban_maintained_miles": row_info.urban_maintained_miles_column,
                    "total_maintained_mile": row_info.total_maintained_miles_column,
                    "rural_dvmt": row_info.rural_dvmt_column,
                    "urban_dvmt": row_info.urban_dvmt_column,
                    "total_dvmt": row_info.total_dvmt_column,
                }
                return (
                    pl.read_excel(
                        source=self.path,
                        sheet_name=self.jurisdiction_sheet_name,
                        read_options={
                            "header_row": None,
                            "column_names": list(columns.keys()),
                            "use_columns": ",".join(list(columns.values())),  # pyright: ignore[reportCallIssue, reportArgumentType]
                            "skip_rows": row_info.row_number - 1,
                            "n_rows": 1,
                        },
                    )
                    .cast(
                        {
                            "urban_maintained_miles": pl.Float64,
                            "urban_dvmt": pl.Float64,
                            "rural_maintained_miles": pl.Float64,
                            "rural_dvmt": pl.Float64,
                            "total_maintained_mile": pl.Float64,
                            "total_dvmt": pl.Float64,
                        }
                    )
                    .select(
                        year=pl.date(self.year, 1, 1),
                        jurisdiction=pl.lit(owner_info.name),
                        jurisdiction_group=pl.lit(owner_info.group),
                        urban_maintained_miles="urban_maintained_miles",
                        urban_dvmt="urban_dvmt",
                        rural_maintained_miles="rural_maintained_miles",
                        rural_dvmt="rural_dvmt",
                        total_maintained_mile="total_maintained_mile",
                        total_dvmt="total_dvmt",
                        source_xlsx=pl.lit(str(self.path)),
                        source_sheet=pl.lit(str(self.jurisdiction_sheet_name)),
                        source_row=pl.lit(row_info.row_number),
                        source_name="jurisdiction",
                    )
                )
            case MPOInfo():
                columns = {
                    "mpo": row_info.mpo_column,
                    "total_maintained_mile": row_info.total_maintained_miles_column,
                    "total_lane_miles": row_info.total_lane_miles_column,
                    "total_dvmt": row_info.total_dvmt_column,
                }
                return (
                    pl.read_excel(
                        source=self.path,
                        sheet_name=self.mpo_sheet_name,
                        read_options={
                            "header_row": None,
                            "column_names": list(columns.keys()),
                            "use_columns": ",".join(list(columns.values())),  # pyright: ignore[reportCallIssue, reportArgumentType]
                            "skip_rows": row_info.row_number - 1,
                            "n_rows": 1,
                        },
                    )
                    .cast(
                        {
                            "total_maintained_mile": pl.Float64,
                            "total_lane_miles": pl.Float64,
                            "total_dvmt": pl.Float64,
                        }
                    )
                    .select(
                        year=pl.date(self.year, 1, 1),
                        mpo=pl.lit(owner_info.name),
                        mpo_brief=pl.lit(owner_info.abbreviation),
                        total_maintained_mile="total_maintained_mile",
                        total_lane_miles="total_lane_miles",
                        total_dvmt="total_dvmt",
                        source_xlsx=pl.lit(str(self.path)),
                        source_sheet=pl.lit(str(self.mpo_sheet_name)),
                        source_row=pl.lit(row_info.row_number),
                        source_name="mpo",
                    )
                )


class PublicRoadDataPDFSource(pydantic.BaseModel):
    year: int
    pdf_path: Path
    pdf_jurisdiction_page: int
    pdf_mpo_page: int
    extract_path: Path
    extract_jurisdiction_sheet_name: str
    extract_mpo_sheet_name: str
    description: str

    def extract_row(
        self,
        owner_info: JurisdictionInfo | MPOInfo,
        row_info: RowInfo,
    ) -> pl.DataFrame:
        match owner_info:
            case JurisdictionInfo():
                return PublicRoadDataExcelSource(
                    year=self.year,
                    path=self.extract_path,
                    jurisdiction_sheet_name=self.extract_jurisdiction_sheet_name,
                    description=self.description,
                ).extract_row(owner_info, row_info)
            case MPOInfo():
                return PublicRoadDataExcelSource(
                    year=self.year,
                    path=self.extract_path,
                    mpo_sheet_name=self.extract_mpo_sheet_name,
                    description=self.description,
                ).extract_row(owner_info, row_info)


type PublicRoadDataSource = PublicRoadDataExcelSource | PublicRoadDataPDFSource


JURISDICTIONS: dict[str, Any] = {
    "CARLSBAD": JurisdictionInfo(name="City of Carlsbad", group="Local"),
    "CHULA_VISTA": JurisdictionInfo(name="City of Chula Vista", group="Local"),
    "CORONADO": JurisdictionInfo(name="City of Coronado", group="Local"),
    "DEL_MAR": JurisdictionInfo(name="City of Del Mar", group="Local"),
    "EL_CAJON": JurisdictionInfo(name="City of El Cajon", group="Local"),
    "ENCINITAS": JurisdictionInfo(name="City of Encinitas", group="Local"),
    "ESCONDIDO": JurisdictionInfo(name="City of Escondido", group="Local"),
    "IMPERIAL_BEACH": JurisdictionInfo(name="City of Imperial Beach", group="Local"),
    "LA_MESA": JurisdictionInfo(name="City of La Mesa", group="Local"),
    "LEMON_GROVE": JurisdictionInfo(name="City of Lemon Grove", group="Local"),
    "NATIONAL_CITY": JurisdictionInfo(name="National City", group="Local"),
    "OCEANSIDE": JurisdictionInfo(name="City of Oceanside", group="Local"),
    "POWAY": JurisdictionInfo(name="City of Poway", group="Local"),
    "SAN_DIEGO": JurisdictionInfo(name="City of San Diego", group="Local"),
    "SAN_MARCOS": JurisdictionInfo(name="City of San Marcos", group="Local"),
    "SANTEE": JurisdictionInfo(name="City of Santee", group="Local"),
    "SOLANA_BEACH": JurisdictionInfo(name="City of Solana Beach", group="Local"),
    "VISTA": JurisdictionInfo(name="City of Vista", group="Local"),
    "UNINCORPORATED": JurisdictionInfo(name="Unincorporated", group="Local"),
    "STATE_HIGHWAY": JurisdictionInfo(name="Caltrans", group="State"),
    "STATE_PARKS_AND_REC": JurisdictionInfo(
        name="California Parks and Recreation", group="State"
    ),
    "OTHER_STATE_AGENCIES": JurisdictionInfo(
        name="Other State Agencies", group="State"
    ),
    "US_BUREAU_OF_INDIAN_AFFAIRS": JurisdictionInfo(
        name="U.S. Bureau of Indian Affairs", group="Federal"
    ),
    "US_DEPARTMENT_OF_DEFENSE": JurisdictionInfo(
        name="U.S. Department of Defense", group="Federal"
    ),
    "US_MILITARY": JurisdictionInfo(name="U.S. Military", group="Federal"),
    "US_FOREST_SERVICE": JurisdictionInfo(name="U.S. Forest Service", group="Federal"),
    "US_NATIONAL_PARK_SERVICE": JurisdictionInfo(
        name="U.S. National Park Service", group="Federal"
    ),
    "US_FISH_AND_WILDLIFE": JurisdictionInfo(
        name="U.S. Fish and Wildlife Service",
        group="Federal",
    ),
    "SAN_DIEGO_UNIFIED_PORT_AUTHORITY": JurisdictionInfo(
        name="San Diego Unified Port Authority",
        group="Other",
    ),
    "SAN_DIEGO_UNIFIED_PORT_DISTRICT": JurisdictionInfo(
        name="San Diego Unified Port District",
        group="Other",
    ),
    "INDIAN_TRIBAL_NATION": JurisdictionInfo(
        name="Indian Tribal Nation",
        group="Other",
    ),
    "OFA": JurisdictionInfo(
        name="OFA",
        group="Other",
    ),
    "US_ARMY": JurisdictionInfo(
        name="U.S. Army",
        group="Federal",
    ),
    "US_MARINE_CORPS": JurisdictionInfo(
        name="U.S. Marine Corps",
        group="Federal",
    ),
    "US_ARMY_AND_MARINE_CORPS": JurisdictionInfo(
        name="U.S. Army/Marine Corps",
        group="Federal",
    ),
    "US_NAVY": JurisdictionInfo(
        name="U.S. Navy",
        group="Federal",
    ),
    "US_BUREAU_OF_LAND_MANAGEMENT": JurisdictionInfo(
        name="U.S. Bureau of Land Management",
        group="Federal",
    ),
}


MPOS = {
    "AMBAG": MPOInfo(
        name="Association of Monterey Bay Area Governments", abbreviation="AMBAG"
    ),
    "BCAG": MPOInfo(
        name="Butte County Association of Governments", abbreviation="BCAG"
    ),
    "FCOG": MPOInfo(name="Fresno Council of Governments", abbreviation="FCOG"),
    "KCAG": MPOInfo(
        name="Kings County Association of Governments", abbreviation="KCAG"
    ),
    "KCOG": MPOInfo(name="Kern Council of Governments", abbreviation="KCOG"),
    "MCAG": MPOInfo(
        name="Merced County Association of Governments", abbreviation="MCAG"
    ),
    "MCTC": MPOInfo(
        name="Madera County Transportation Commission", abbreviation="MCTC"
    ),
    "MTC": MPOInfo(name="Metropolitan Transportation Commission", abbreviation="MTC"),
    "SACOG": MPOInfo(
        name="Sacramento Area Council of Governments", abbreviation="SACOG"
    ),
    "SANDAG": MPOInfo(
        name="San Diego Association of Governments", abbreviation="SANDAG"
    ),
    "SBCAG": MPOInfo(
        name="Santa Barbara County Association of Governments", abbreviation="SBCAG"
    ),
    "SCAG": MPOInfo(
        name="Southern California Association of Governments", abbreviation="SCAG"
    ),
    "SJCOG": MPOInfo(name="San Joaquin Council of Governments", abbreviation="SJCOG"),
    "SLOCOG": MPOInfo(
        name="San Luis Obispo Council of Governments", abbreviation="SLOCOG"
    ),
    "SRTA": MPOInfo(name="Shasta Regional Transportation Agency", abbreviation="SRTA"),
    "STANCOG": MPOInfo(
        name="Stanislaus Council of Governments", abbreviation="StanCOG"
    ),
    "TCAG": MPOInfo(
        name="Tulare County Association of Governments", abbreviation="TCAG"
    ),
    "TRPA": MPOInfo(name="Tahoe Regional Planning Agency", abbreviation="TRPA"),
    "NONE": MPOInfo(name="Not in any MPO", abbreviation="None"),
}
