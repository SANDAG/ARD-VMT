from pathlib import Path
from typing import Literal

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
                            "use_columns": ",".join(list(columns.values())),
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
                            "use_columns": ",".join(list(columns.values())),
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
