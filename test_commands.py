
from rtdms_redis_timeseries.redis_timeseries_utility import RedisTimeSeries, PostgresLogger

redis_obj = RedisTimeSeries()
post_obj = PostgresLogger()
# sample data for insertion
ingestion_stream = {
        "datapoints": [
            [1693298532000, 33],
            [1693298544000, 100],
            ],
        "site": "site_101",
        "parameter": "parameter_3",
        "t": 9,
        "factor": "U"
    }

try:
    # insertion -- ts.create will create the timeseries skeleton. The future datapoints will carry the same labels that we specify in ts.create

    # result = redis_obj.insert_bulk_datapoints(timeseries_key="ganga_live_data",
    #                                           data=ingestion_stream, duplicate_policy="last")

    # aggregation on a single timeseries key. Mandatory to specify the time_series_key
    # result = redis_obj.aggregate_one_timeseries(start_time=1671940300000, end_time=1698960200120,
    #                                             time_series_key="glens:analyzer_642:CEMS_3:Emission:parameter_3:U:site_4041",
    #                                             filter_labels={}, aggregations={"avg": '2hours'})

    # aggregation on all timeseries keys. Label values are must for filtering.
    # Result is timeseries key specific use grouping to combine

    # result = redis_obj.aggregate_all_timeseries(start_time=1671940300000, end_time=1698960200120,
    #                                             filter_labels={"site_id": "site_4041"}, aggregations={"avg": "1hours"},
    #                                             show_labels=True,
    #                                             group_by_and_reduce={"group_by": "parameter_id", "reduce": "avg"})

    # # deleting data in a timeseries key for a specific range
    # result = redis_obj.delete_data(timeseries_key="glens:analyzer_642:CEMS_3:Emission:parameter_3:U:site_4041",
    #                                from_timestamp=1678950300000, to_timestamp=1678960200000)

    # executes our own rts query

    result = post_obj.redis_db_cleaner()
    print(result)
except Exception as e:
    print(e)





