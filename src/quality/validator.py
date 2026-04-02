from __future__ import annotations

import polars as pl


def validate_quality(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    duplicate_mask = pl.struct(
        ["device_id", "ts_utc", "latitude", "longitude", "accuracy_m"]
    ).is_duplicated()

    reason_expr = (
        pl.when(pl.col("device_id").is_null() | (pl.col("device_id").str.strip_chars() == ""))
        .then(pl.lit("NULL_DEVICE_ID"))
        .otherwise(pl.lit(None))
        .alias("r_device")
    )

    checks_df = df.with_columns(
        [
            reason_expr,
            pl.when(pl.col("ts_utc").is_null() | (pl.col("ts_utc").str.strip_chars() == ""))
            .then(pl.lit("INVALID_TIMESTAMP"))
            .otherwise(pl.lit(None))
            .alias("r_ts"),
            pl.when(pl.col("latitude").is_null() | pl.col("longitude").is_null())
            .then(pl.lit("NULL_COORDINATE"))
            .otherwise(pl.lit(None))
            .alias("r_null_coord"),
            pl.when(
                pl.col("latitude").is_not_null()
                & ((pl.col("latitude") < -90.0) | (pl.col("latitude") > 90.0))
            )
            .then(pl.lit("INVALID_LATITUDE_RANGE"))
            .otherwise(pl.lit(None))
            .alias("r_lat"),
            pl.when(
                pl.col("longitude").is_not_null()
                & ((pl.col("longitude") < -180.0) | (pl.col("longitude") > 180.0))
            )
            .then(pl.lit("INVALID_LONGITUDE_RANGE"))
            .otherwise(pl.lit(None))
            .alias("r_lon"),
            pl.when(duplicate_mask).then(pl.lit("EXACT_DUPLICATE")).otherwise(pl.lit(None)).alias("r_dup"),
        ]
    ).with_columns(
        pl.concat_str(
            ["r_device", "r_ts", "r_null_coord", "r_lat", "r_lon", "r_dup"],
            separator="|",
            ignore_nulls=True,
        ).alias("reject_reason")
    )

    accepted = checks_df.filter(pl.col("reject_reason") == "").drop(
        ["r_device", "r_ts", "r_null_coord", "r_lat", "r_lon", "r_dup", "reject_reason"]
    )
    rejected = checks_df.filter(pl.col("reject_reason") != "").drop(
        ["r_device", "r_ts", "r_null_coord", "r_lat", "r_lon", "r_dup"]
    )
    return accepted, rejected


def quality_counts_by_reason(rejected: pl.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    if rejected.height == 0:
        return counts
    reasons = rejected.get_column("reject_reason").to_list()
    for reason_group in reasons:
        if reason_group is None:
            continue
        for reason in str(reason_group).split("|"):
            if reason == "":
                continue
            counts[reason] = counts.get(reason, 0) + 1
    return counts

