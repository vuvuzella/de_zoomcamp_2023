from pydantic import BaseModel
from datetime import datetime
from decimal import Decimal
from typing import Optional


class Ride(BaseModel):

    vendor_id: str
    store_and_fwd_flag: str
    rate_code_id: Optional[int]
    pu_location_id: int
    do_location_id: int
    passenger_count: int
    trip_distance: Decimal
    fare_amount: Decimal
    extra: Decimal
    mta_tax: Decimal
    tip_amount: Decimal
    tolls_amount: Decimal
    ehail_fee: str
    improvement_surcharge: Decimal
    total_amount: Decimal
    payment_type: int
    trip_type: int
    congestion_surcharge: Optional[str]

    def __init__(self, **data) -> None:
        vendor_id = data.pop("VendorID")
        store_and_fwd_flag = data.pop("store_and_fwd_flag")
        rate_code_id = data.pop("RatecodeID")
        pu_location_id = data.pop("PULocationID")
        do_location_id = data.pop("DOLocationID")
        passenger_count = data.pop("passenger_count")
        trip_distance = data.pop("trip_distance")
        fare_amount = data.pop("fare_amount")
        extra = data.pop("extra")
        mta_tax = data.pop("mta_tax")
        tip_amount = data.pop("tip_amount")
        tolls_amount = data.pop("tolls_amount")
        ehail_fee = data.pop("ehail_fee")
        improvement_surcharge = data.pop("improvement_surcharge")
        total_amount = data.pop("total_amount")
        payment_type = data.pop("payment_type")
        trip_type = data.pop("trip_type")
        congestion_surcharge = data.pop("congestion_surcharge")

        super().__init__(
            vendor_id=vendor_id,
            store_and_fwd_flag=store_and_fwd_flag,
            rate_code_id=rate_code_id,
            pu_location_id=pu_location_id,
            do_location_id=do_location_id,
            passenger_count=passenger_count,
            trip_distance=trip_distance,
            fare_amount=fare_amount,
            extra=extra,
            mta_tax=mta_tax,
            tip_amount=tip_amount,
            tolls_amount=tolls_amount,
            ehail_fee=ehail_fee,
            improvement_surcharge=improvement_surcharge,
            total_amount=total_amount,
            payment_type=payment_type,
            trip_type=trip_type,
            congestion_surcharge=congestion_surcharge,
            **data
        )


class GreenRide(Ride):

    lpep_pickup_datetime: datetime
    lpep_dropoff_datetime: datetime

    def __init__(self, **data) -> None:
        # pop the key off the dictionary to be able to add **data in the kwargs of the parent class
        lpep_pickup_datetime = data.pop("lpep_pickup_datetime", None)
        lpep_dropoff_datetime = data.pop("lpep_dropoff_datetime", None)

        super().__init__(
            lpep_pickup_datetime=lpep_pickup_datetime,
            lpep_dropoff_datetime=lpep_dropoff_datetime,
            **data
        )


class YellowRide(Ride):

    tpep_pickup_datetime: datetime
    tpep_dropoff_datetime: datetime

    def __init__(self, **data) -> None:
        tpep_pickup_datetime = data.pop("tpep_pickup_datetime")
        tpep_dropoff_datetime = data.pop("tpep_dropoff_datetime")
        super().__init__(
            tpep_pickup_datetime=tpep_pickup_datetime,
            tpep_dropoff_datetime=tpep_dropoff_datetime,
            **data
        )
