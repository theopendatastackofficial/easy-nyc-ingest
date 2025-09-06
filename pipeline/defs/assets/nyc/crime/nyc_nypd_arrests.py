# pipeline/defs/assets/nyc/crime/nyc_nypd_arrests.py

import polars as pl
from dagster import AssetKey, asset
from pipeline.constants import CLEAN_LAKE_PATH

TAG_CRIME = {"domain": "nypd", "team": "public-safety", "priority": "high"}


@asset(
    name="nyc_nypd_arrests",
    deps=[
        AssetKey("nyc_nypd_arrests_historical"),
        AssetKey("nyc_nypd_arrests_ytd"),
    ],
    io_manager_key="clean_single_io_manager",
    group_name="crime",
    tags={**TAG_CRIME, "aggregation": "combined"},
    metadata={"r2_type": "single"},
)
def nyc_nypd_arrests():
    """
    Combine historical arrests with year-to-date arrests into a single DataFrame,
    ensuring both partitions share the same schema (column names + types).
    Then, compute two new columns:
      - 'clean_offense_desc' via a Polars join against a lookup table
      - 'boro' via Polars SQL on 'arrest_boro'

    Finally, materialize the result via the configured IO manager.
    """

    # ------------------------------------------------------------------------
    # 1) Load historical and YTD Parquet files as Polars DataFrames
    # ------------------------------------------------------------------------
    df_hist = (
        pl.scan_parquet(
            "/home/christiandata/nyc-backfill-datasets/data/opendata/clean/"
            "nyc_nypd_arrests_historical/*.parquet"
        )
        .collect()
    )
    df_ytd = (
        pl.scan_parquet(
            "/home/christiandata/nyc-backfill-datasets/data/opendata/clean/"
            "nyc_nypd_arrests_ytd/*.parquet"
        )
        .collect()
    )

    # ------------------------------------------------------------------------
    # 2) Compute which columns each frame is missing
    # ------------------------------------------------------------------------
    hist_cols = set(df_hist.columns)
    ytd_cols = set(df_ytd.columns)

    # 2a) If df_hist is missing a column that df_ytd has, add it to df_hist
    for col in ytd_cols - hist_cols:
        target_dtype = df_ytd.schema[col]
        df_hist = df_hist.with_columns(
            [pl.lit(None).cast(target_dtype).alias(col)]
        )

    # 2b) If df_ytd is missing a column that df_hist has, add it to df_ytd
    for col in hist_cols - ytd_cols:
        target_dtype = df_hist.schema[col]
        df_ytd = df_ytd.with_columns(
            [pl.lit(None).cast(target_dtype).alias(col)]
        )

    # ------------------------------------------------------------------------
    # 3) Reorder both frames to share the exact same column ordering
    # ------------------------------------------------------------------------
    unified_cols = sorted(df_hist.columns)
    df_hist = df_hist.select(unified_cols)
    df_ytd = df_ytd.select(unified_cols)

    # ------------------------------------------------------------------------
    # 4) Concatenate vertically now that schemas fully match
    # ------------------------------------------------------------------------
    df_combined = pl.concat([df_hist, df_ytd], how="vertical")

    # ------------------------------------------------------------------------
    # 5) Define the offense-description → clean-category mapping as a lookup table
    # ------------------------------------------------------------------------
    mapping_dict = {
        # Violent Crimes
        "ASSAULT 3 & RELATED OFFENSES": "Violent Crimes",
        "FELONY ASSAULT":              "Violent Crimes",
        "MURDER & NON-NEGL. MANSLAUGHTER": "Violent Crimes",
        "ROBBERY":                     "Violent Crimes",
        "KIDNAPPING & RELATED OFFENSES":   "Violent Crimes",
        "OFFENSES AGAINST THE PERSON":     "Violent Crimes",

        # Property Crimes
        "ARSON":                       "Property Crimes",
        "BURGLARY":                    "Property Crimes",
        "BURGLAR'S TOOLS":             "Property Crimes",
        "CRIMINAL MISCHIEF & RELATED OFFENSES": "Property Crimes",
        "CRIMINAL TRESPASS":          "Property Crimes",
        "GRAND LARCENY":              "Property Crimes",
        "GRAND LARCENY OF MOTOR VEHICLE": "Property Crimes",
        "PETIT LARCENY":              "Property Crimes",
        "POSSESSION OF STOLEN PROPERTY": "Property Crimes",
        "POSSESSION OF STOLEN PROPERTY 5": "Property Crimes",
        "THEFT-FRAUD":                "Property Crimes",
        "OTHER OFFENSES RELATED TO THEFT": "Property Crimes",

        # Drug Offenses
        "DANGEROUS DRUGS":            "Drug Offenses",

        # Weapons Offenses
        "DANGEROUS WEAPONS":          "Weapons Offenses",

        # Fraud & Financial Crimes
        "FORGERY":                    "Fraud & Financial Crimes",
        "OFFENSES INVOLVING FRAUD":   "Fraud & Financial Crimes",
        "FRAUDS":                     "Fraud & Financial Crimes",
        "FRAUDULENT ACCOSTING":       "Fraud & Financial Crimes",

        # Sex & Sexual Offenses
        "SEX CRIMES":                 "Sex & Sexual Offenses",
        "RAPE":                       "Sex & Sexual Offenses",
        "FORCIBLE TOUCHING":          "Sex & Sexual Offenses",
        "PROSTITUTION & RELATED OFFENSES": "Sex & Sexual Offenses",

        # Traffic & DUI Offenses
        "VEHICLE AND TRAFFIC LAWS":   "Traffic & DUI Offenses",
        "MOVING INFRACTIONS":         "Traffic & DUI Offenses",
        "OTHER TRAFFIC INFRACTION":   "Traffic & DUI Offenses",
        "INTOXICATED & IMPAIRED DRIVING": "Traffic & DUI Offenses",
        "INTOXICATED/IMPAIRED DRIVING":   "Traffic & DUI Offenses",
        "UNAUTHORIZED USE OF A VEHICLE 3 (UUV)": "Traffic & DUI Offenses",
        "ALCOHOLIC BEVERAGE CONTROL LAW":       "Traffic & DUI Offenses",

        # Public Order & Other Offenses
        "ADMINISTRATIVE CODE":        "Public Order & Other Offenses",
        "ANTICIPATORY OFFENSES":      "Public Order & Other Offenses",
        "DISORDERLY CONDUCT":         "Public Order & Other Offenses",
        "F.C.A. P.I.N.O.S.":          "Public Order & Other Offenses",
        "HARRASSMENT 2":              "Public Order & Other Offenses",
        "LOITERING":                  "Public Order & Other Offenses",
        "LOITERING/GAMBLING (CARDS, DICE, ETC)": "Public Order & Other Offenses",
        "GAMBLING":                   "Public Order & Other Offenses",
        "MISCELLANEOUS PENAL LAW":    "Public Order & Other Offenses",
        "OFF. AGNST PUB ORD SENSBLTY & RGHTS TO PRIV": "Public Order & Other Offenses",
        "OFFENSES AGAINST PUBLIC ADMINISTRATION":     "Public Order & Other Offenses",
        "OFFENSES RELATED TO CHILDREN":               "Public Order & Other Offenses",
        "OTHER STATE LAWS":          "Public Order & Other Offenses",
        "OTHER STATE LAWS (NON PENAL LAW)": "Public Order & Other Offenses",
        "CHILD ABANDONMENT/NON SUPPORT 1": "Public Order & Other Offenses",
    }

    # Build a small mapping DataFrame:
    mapping_df = pl.DataFrame(
        {
            "ofns_desc": list(mapping_dict.keys()),
            "clean_offense_desc": list(mapping_dict.values()),
        }
    )

    # ------------------------------------------------------------------------
    # 6) Join the lookup onto df_combined to assign clean_offense_desc, then fill nulls
    # ------------------------------------------------------------------------
    df_with_clean = (
        df_combined
        # Left‐join on raw ofns_desc; any unmatched ofns_desc will have null for clean_offense_desc
        .join(mapping_df, on="ofns_desc", how="left")
        # Fill any null clean_offense_desc with "Other"
        .with_columns(
            pl.col("clean_offense_desc").fill_null("Other")
        )
    )

    # ------------------------------------------------------------------------
    # 7) Compute 'boro' via Polars SQL; SELECT * carries forward clean_offense_desc
    # ------------------------------------------------------------------------
    boro_sql = """
    SELECT
        *,

        CASE arrest_boro
            WHEN 'B' THEN 'Bronx'
            WHEN 'S' THEN 'Staten Island'
            WHEN 'K' THEN 'Brooklyn'
            WHEN 'M' THEN 'Manhattan'
            WHEN 'Q' THEN 'Queens'
            ELSE 'Unknown'
        END AS boro

    FROM self
    """

    df_with_boro = df_with_clean.lazy().sql(boro_sql).collect()

    # ------------------------------------------------------------------------
    # 8) Return the final DataFrame. Dagster will materialize it per your IO Manager.
    # ------------------------------------------------------------------------
    return df_with_boro
