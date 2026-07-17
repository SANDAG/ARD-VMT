import polars as pl

from vmt.hpms.vintages import (
    prd1996,
    prd1997,
    prd1998,
    prd1999,
    prd2000,
    prd2001,
    prd2002,
    prd2003,
    prd2004,
    prd2005,
    prd2006,
    prd2007,
    prd2008,
    prd2009,
    prd2010,
    prd2011,
    prd2012,
    prd2013,
    prd2014,
    prd2015,
    prd2016,
    prd2017,
    prd2018,
    prd2019,
    prd2020,
    prd2021,
    prd2022,
    prd2023,
    prd2024,
)

if __name__ == "__main__":
    hpms_jurisdictions = pl.concat(
        [
            pl.concat(prd1996.JURISDICTION_EXTRACTS),
            pl.concat(prd1997.JURISDICTION_EXTRACTS),
            pl.concat(prd1998.JURISDICTION_EXTRACTS),
            pl.concat(prd1999.JURISDICTION_EXTRACTS),
            pl.concat(prd2000.JURISDICTION_EXTRACTS),
            pl.concat(prd2001.JURISDICTION_EXTRACTS),
            pl.concat(prd2002.JURISDICTION_EXTRACTS),
            pl.concat(prd2003.JURISDICTION_EXTRACTS),
            pl.concat(prd2004.JURISDICTION_EXTRACTS),
            pl.concat(prd2005.JURISDICTION_EXTRACTS),
            pl.concat(prd2006.JURISDICTION_EXTRACTS),
            pl.concat(prd2007.JURISDICTION_EXTRACTS),
            pl.concat(prd2008.JURISDICTION_EXTRACTS),
            pl.concat(prd2009.JURISDICTION_EXTRACTS),
            pl.concat(prd2010.JURISDICTION_EXTRACTS),
            pl.concat(prd2011.JURISDICTION_EXTRACTS),
            pl.concat(prd2012.JURISDICTION_EXTRACTS),
            pl.concat(prd2013.JURISDICTION_EXTRACTS),
            pl.concat(prd2014.JURISDICTION_EXTRACTS),
            pl.concat(prd2015.JURISDICTION_EXTRACTS),
            pl.concat(prd2016.JURISDICTION_EXTRACTS),
            pl.concat(prd2017.JURISDICTION_EXTRACTS),
            pl.concat(prd2018.JURISDICTION_EXTRACTS),
            pl.concat(prd2019.JURISDICTION_EXTRACTS),
            pl.concat(prd2020.JURISDICTION_EXTRACTS),
            pl.concat(prd2021.JURISDICTION_EXTRACTS),
            pl.concat(prd2022.JURISDICTION_EXTRACTS),
            pl.concat(prd2023.JURISDICTION_EXTRACTS),
            pl.concat(prd2024.JURISDICTION_EXTRACTS),
        ]
    )
    df = hpms_jurisdictions.group_by("jurisdiction", "jurisdiction_group").agg(
        pl.col("jurisdiction").count().alias("n"),
        pl.col("year").min().alias("year_min"),
        pl.col("year").max().alias("year_max"),
    )
    print(df.filter(pl.col("jurisdiction_group") == "Local"))
    print(df.filter(pl.col("jurisdiction_group") == "State"))
    print(df.filter(pl.col("jurisdiction_group") == "Federal"))
    print(df.filter(pl.col("jurisdiction_group") == "Other"))
    print(hpms_jurisdictions.filter(pl.col("year") == pl.col("year").max()))
    _ = hpms_jurisdictions.write_parquet(
        "./data/hpms/clean/hpms.jurisdiction_dvmt.parquet"
    )
    _ = hpms_jurisdictions.write_excel("./data/hpms/clean/hpms.jurisdiction_dvmt.xlsx")

    hpms_mpos = pl.concat(
        [
            pl.concat(prd2001.MPO_EXTRACTS),
            pl.concat(prd2002.MPO_EXTRACTS),
            pl.concat(prd2003.MPO_EXTRACTS),
            pl.concat(prd2004.MPO_EXTRACTS),
            pl.concat(prd2005.MPO_EXTRACTS),
            pl.concat(prd2006.MPO_EXTRACTS),
            pl.concat(prd2007.MPO_EXTRACTS),
            pl.concat(prd2008.MPO_EXTRACTS),
            pl.concat(prd2009.MPO_EXTRACTS),
            pl.concat(prd2010.MPO_EXTRACTS),
            pl.concat(prd2011.MPO_EXTRACTS),
            pl.concat(prd2012.MPO_EXTRACTS),
            pl.concat(prd2013.MPO_EXTRACTS),
            pl.concat(prd2014.MPO_EXTRACTS),
            pl.concat(prd2015.MPO_EXTRACTS),
            pl.concat(prd2016.MPO_EXTRACTS),
            pl.concat(prd2017.MPO_EXTRACTS),
            pl.concat(prd2018.MPO_EXTRACTS),
            pl.concat(prd2019.MPO_EXTRACTS),
            pl.concat(prd2020.MPO_EXTRACTS),
            pl.concat(prd2021.MPO_EXTRACTS),
            pl.concat(prd2022.MPO_EXTRACTS),
            pl.concat(prd2023.MPO_EXTRACTS),
            pl.concat(prd2024.MPO_EXTRACTS),
        ]
    )
    df = hpms_mpos.group_by("mpo_brief", "mpo").agg(
        pl.col("mpo").count().alias("n"),
        pl.col("year").min().alias("year_min"),
        pl.col("year").max().alias("year_max"),
    )
    print(hpms_mpos.filter(pl.col("year") == pl.col("year").max()))
    print(df)
    _ = hpms_mpos.write_parquet("./data/hpms/clean/hpms.mpo_dvmt.parquet")
    _ = hpms_mpos.write_excel("./data/hpms/clean/hpms.mpo_dvmt.xlsx")
