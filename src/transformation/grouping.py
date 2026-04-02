from __future__ import annotations

import polars as pl

from src.domain.models import StayType, VisitKind

EARTH_RADIUS_M = 6_371_000.0


def _haversine_distance_m_expr(
    lat_expr: pl.Expr,
    lon_expr: pl.Expr,
    prev_lat_expr: pl.Expr,
    prev_lon_expr: pl.Expr,
) -> pl.Expr:
    lat1 = lat_expr.radians()
    lon1 = lon_expr.radians()
    lat2 = prev_lat_expr.radians()
    lon2 = prev_lon_expr.radians()

    dlat = lat1 - lat2
    dlon = lon1 - lon2
    sin_dlat = (dlat / 2).sin()
    sin_dlon = (dlon / 2).sin()
    a = sin_dlat.pow(2) + lat1.cos() * lat2.cos() * sin_dlon.pow(2)
    c = 2 * a.sqrt().arcsin()
    return pl.when(prev_lat_expr.is_null() | prev_lon_expr.is_null()).then(
        pl.lit(0.0)
    ).otherwise(pl.lit(EARTH_RADIUS_M) * c)


def classify_stay_type(
    visit_kind_expr: pl.Expr,
    start_hour_expr: pl.Expr,
    end_hour_expr: pl.Expr,
    duration_seconds_expr: pl.Expr,
) -> pl.Expr:
    is_stay = visit_kind_expr == VisitKind.STAY.value
    is_home_window = (start_hour_expr >= 20) | (end_hour_expr <= 8)
    is_work_window = (
        (start_hour_expr >= 7)
        & (start_hour_expr <= 11)
        & (end_hour_expr >= 12)
        & (end_hour_expr <= 20)
    )

    return (
        pl.when(~is_stay)
        .then(pl.lit(None))
        .when(is_home_window & (duration_seconds_expr >= 7_200))
        .then(pl.lit(StayType.HOME.value))
        .when(is_work_window & (duration_seconds_expr >= 10_800))
        .then(pl.lit(StayType.WORK.value))
        .otherwise(pl.lit(StayType.OTHER.value))
        .alias("stay_type")
    )


def group_pings_into_visits(
    accepted_df: pl.DataFrame,
    *,
    max_gap_seconds: int = 900,
    max_distance_m: float = 250.0,
    stay_min_duration_seconds: int = 600,
    stay_min_pings: int = 3,
) -> pl.DataFrame:
    if accepted_df.height == 0:
        return pl.DataFrame(
            schema={
                "visit_id": pl.String,
                "device_id": pl.String,
                "visit_kind": pl.String,
                "stay_type": pl.String,
                "start_ts_utc": pl.String,
                "end_ts_utc": pl.String,
                "duration_seconds": pl.Int64,
                "ping_count": pl.Int64,
                "representative_latitude": pl.Float64,
                "representative_longitude": pl.Float64,
            }
        )

    parsed = (
        accepted_df.with_columns(
            pl.col("ts_utc").str.strptime(
                pl.Datetime(time_zone="UTC"),
                format="%Y-%m-%dT%H:%M:%SZ",
                strict=False,
            ).alias("ts_dt")
        )
        .filter(pl.col("ts_dt").is_not_null())
        .sort(["device_id", "ts_dt"])
        .with_columns(
            [
                pl.col("ts_dt").shift(1).over("device_id").alias("prev_ts_dt"),
                pl.col("latitude").shift(1).over("device_id").alias("prev_latitude"),
                pl.col("longitude").shift(1).over("device_id").alias("prev_longitude"),
            ]
        )
        .with_columns(
            [
                (pl.col("ts_dt") - pl.col("prev_ts_dt")).dt.total_seconds().alias("gap_seconds"),
                _haversine_distance_m_expr(
                    pl.col("latitude"),
                    pl.col("longitude"),
                    pl.col("prev_latitude"),
                    pl.col("prev_longitude"),
                ).alias("distance_from_prev_m"),
            ]
        )
        .with_columns(
            (
                pl.col("prev_ts_dt").is_null()
                | (pl.col("gap_seconds") > max_gap_seconds)
                | (pl.col("distance_from_prev_m") > max_distance_m)
            )
            .cast(pl.Int64)
            .cum_sum()
            .over("device_id")
            .alias("visit_seq")
        )
    )

    grouped = (
        parsed.group_by(["device_id", "visit_seq"])
        .agg(
            [
                pl.col("ts_dt").min().alias("start_dt"),
                pl.col("ts_dt").max().alias("end_dt"),
                pl.len().alias("ping_count"),
                pl.col("latitude").median().alias("representative_latitude"),
                pl.col("longitude").median().alias("representative_longitude"),
            ]
        )
        .with_columns(
            [
                (pl.col("end_dt") - pl.col("start_dt")).dt.total_seconds().cast(pl.Int64).alias(
                    "duration_seconds"
                ),
            ]
        )
        .with_columns(
            [
                pl.when(
                    (pl.col("duration_seconds") >= stay_min_duration_seconds)
                    & (pl.col("ping_count") >= stay_min_pings)
                )
                .then(pl.lit(VisitKind.STAY.value))
                .otherwise(pl.lit(VisitKind.PASS_BY.value))
                .alias("visit_kind"),
                pl.col("start_dt").dt.hour().alias("start_hour"),
                pl.col("end_dt").dt.hour().alias("end_hour"),
            ]
        )
        .with_columns(
            classify_stay_type(
                pl.col("visit_kind"),
                pl.col("start_hour"),
                pl.col("end_hour"),
                pl.col("duration_seconds"),
            )
        )
        .with_columns(
            [
                pl.concat_str(
                    [
                        pl.col("device_id"),
                        pl.lit("_"),
                        pl.col("visit_seq").cast(pl.String),
                        pl.lit("_"),
                        pl.col("start_dt").cast(pl.Int64).cast(pl.String),
                    ]
                ).alias("visit_id"),
                pl.col("start_dt").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("start_ts_utc"),
                pl.col("end_dt").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("end_ts_utc"),
            ]
        )
        .select(
            [
                "visit_id",
                "device_id",
                "visit_kind",
                "stay_type",
                "start_ts_utc",
                "end_ts_utc",
                "duration_seconds",
                "ping_count",
                "representative_latitude",
                "representative_longitude",
            ]
        )
        .sort(["device_id", "start_ts_utc"])
    )
    return grouped

