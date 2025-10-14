from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import panel as pn
import param
import plotly.graph_objects as go
import polars as pl

pn.extension("plotly")
pn.config.template = "fast"


@dataclass
class Datasets:
    pems: pl.DataFrame
    hpms_table_6: pl.DataFrame
    hpms_table_9: pl.DataFrame
    inrix: pl.DataFrame
    emfac: pl.DataFrame

    @lru_cache(maxsize=None)
    @staticmethod
    def load(
        pems_path: Path = Path("./data/clean/pems/pems.parquet"),
        hpms_table_6_path: Path = Path("./data/clean/hpms/table_6.parquet"),
        hpms_table_9_path: Path = Path("./data/clean/hpms/table_9.parquet"),
        inrix_path: Path = Path("./data/clean/inrix/umr2022.parquet"),
        emfac_path: Path = Path("./data/clean/emfac/emfac.parquet"),
    ) -> Datasets:
        return Datasets(
            pems=pl.read_parquet(pems_path),
            hpms_table_6=pl.read_parquet(hpms_table_6_path),
            hpms_table_9=pl.read_parquet(hpms_table_9_path),
            inrix=pl.read_parquet(inrix_path),
            emfac=pl.read_parquet(emfac_path),
        )


class VMTDashboard(param.Parameterized):
    _plotly_pane = pn.pane.Plotly(object=None, sizing_mode="stretch_both")
    _datasets: Datasets = Datasets.load()
    active_datasets = param.ListSelector(
        objects=[
            "PeMS",
            "HPMS (PRD Table 6)",
            "HPMS (PRD Table 9)",
            "INRIX (UMR)",
            "EMFAC",
        ],
        default=["PeMS"],
    )

    def update_plotly_pane(self):
        figure = go.Figure()
        figure.update_traces(
            legendgrouptitle_font_lineposition="under", selector=dict(name="scattergl")
        )

        if "PeMS" in self.active_datasets:
            figure.add_trace(
                go.Scattergl(
                    x=self._datasets.pems["date"],
                    y=self._datasets.pems["vmt"],
                    mode="markers",
                    marker={"size": 1},
                    name="PeMS",
                )
            )

        if "HPMS (PRD Table 6)" in self.active_datasets:
            df = (
                self._datasets.hpms_table_6.group_by(["timestamp"])
                .agg(
                    (
                        (pl.col("dvmt_1000_urban") * 1_000)
                        + (pl.col("dvmt_1000_rural") * 1_000)
                    )
                    .sum()
                    .alias("dvmt")
                )
                .sort("timestamp")
            )
            figure.add_trace(
                go.Scattergl(
                    x=df["timestamp"],
                    y=df["dvmt"],
                    mode="markers+lines",
                    name="HPMS (PRD Table 6)",
                )
            )

        if "HPMS (PRD Table 9)" in self.active_datasets:
            df = (
                self._datasets.hpms_table_9.filter(pl.col("mpo") == "SANDAG")
                .group_by(["timestamp"])
                .agg((pl.col("dvmt_1000")).sum().alias("dvmt") * 1_000)
                .sort("timestamp")
            )
            figure.add_trace(
                go.Scatter(
                    x=df["timestamp"],
                    y=df["dvmt"],
                    mode="markers+lines",
                    name="HPMS (PRD Table 9)",
                ),
            )

        if "INRIX (UMR)" in self.active_datasets:
            df = (
                self._datasets.inrix.filter(pl.col("Urban Area") == "San Diego CA")
                .group_by(["Year"])
                .agg((pl.col("Freeway DVMT")).sum() * 1_000)
                .sort("Year")
            )
            figure.add_trace(
                go.Scattergl(
                    x=df["Year"],
                    y=df["Freeway DVMT"],
                    mode="lines",
                    name="INRIX (UMR) Freeway VMT",
                    line={"width": 2, "dash": "dash"},
                ),
            )
            df = (
                self._datasets.inrix.filter(pl.col("Urban Area") == "San Diego CA")
                .group_by(["Year"])
                .agg((pl.col("Freeway DVMT", "Arterial Street DVMT")).sum() * 1_000)
                .sort("Year")
            )
            figure.add_trace(
                go.Scattergl(
                    x=df["Year"],
                    y=df["Freeway DVMT"] + df["Arterial Street DVMT"],
                    mode="lines",
                    name="INRIX (UMR) Freeway and Arterial Street VMT",
                    line={"width": 2, "dash": "dash"},
                ),
            )
        if "EMFAC" in self.active_datasets:
            for model in ["EMFAC 2017", "EMFAC 2021", "EMFAC 2025"]:
                df = (
                    self._datasets.emfac.filter(pl.col("EMFAC Model") == model)
                    .group_by(["Calendar Year"])
                    .agg((pl.col("Total VMT")).sum())
                    .sort("Calendar Year")
                )
                figure.add_trace(
                    go.Scattergl(
                        x=df["Calendar Year"],
                        y=df["Total VMT"],
                        mode="lines",
                        name=model,
                        line={"width": 2, "dash": "dot"},
                    ),
                )

        self._plotly_pane.object = figure
        return self._plotly_pane

    @param.depends("active_datasets")
    def plotly_pane(self) -> pn.pane.Plotly:
        self._update_plotly_pane()
        return self._plotly_pane


dashboard = VMTDashboard()
pn.Column(
    pn.Param(
        dashboard.param,
        widgets={
            "active_datasets": {
                "widget_type": pn.widgets.CheckButtonGroup,
                "orientation": "vertical",
            }
        },
    )
).servable(target="sidebar")
pn.Column(dashboard.update_plotly_pane).servable(target="main")
