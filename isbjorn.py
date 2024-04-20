from pathlib import Path

from polars import Int64, LazyFrame, col, scan_csv, scan_parquet


def _scan_trips() -> LazyFrame:
    path = Path("../files/trips.parquet")
    return scan_parquet(path)


def _scan_zones() -> LazyFrame:
    path = Path("../files/zones.csv")
    return scan_csv(path)


def trips_to_airports(fare_threshold: int = 0) -> LazyFrame:
    trips = _scan_trips()
    zones = _scan_zones()
    joined = trips.join(
        zones,
        left_on=col("DOLocationID").cast(Int64),
        right_on=col("LocationID"),
    )
    filtered = joined.filter(
        (col("Zone").str.to_lowercase().str.ends_with("airport"))
        & (col("fare_amount") >= fare_threshold)
    )
    lf = filtered.select(
        col("tpep_pickup_datetime").alias("Pickup Time"),
        col("Zone").alias("Destination"),
        col("fare_amount").alias("Base Fare"),
        col("Airport_fee").alias("Airport Surcharge"),
    )
    return lf


if __name__ == "__main__":
    print(trips_to_airports(400).collect())
