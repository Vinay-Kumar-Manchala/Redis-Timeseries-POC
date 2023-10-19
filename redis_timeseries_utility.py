import copy
import json
import logging
import datetime
from typing import Dict
from db_reader import RedisDbReader, SqlDbReader


class RedisTimeSeries:
    def __init__(self):
        self.redis_client = RedisDbReader()

    def create_new_timeseries(self, timeseries_name: str, labels: Dict, duplicate_policy="Block", call_type="creation"):
        """
        This method is for creating a new redis timeseries.

        :param timeseries_name: key specifying the timeseries name
        :param labels: dictionary of key value pairs. similar to tags in kairos
        :param duplicate_policy: Option to choose in case of duplicate timestamps for a particular timeseries key
        :param call_type: set to insert_one for within utility function calls based on the need
        :return: timeseries key
        """
        try:
            if "datapoints" in labels:
                del labels["datapoints"]
            label_pairs = f" LABELS {' '.join(f'{key} {value}' for key, value in labels.items())} " \
                          if labels else ""
            timeseries_name = ":".join([timeseries_name] + [str(item) for item in labels.values()])

            if call_type == "insert_one":
                return f"TS.ADD {timeseries_name} _timestamp _value DUPLICATE_POLICY {duplicate_policy} {label_pairs}",\
                         timeseries_name

            else:
                with self.redis_client.redis_connect() as rts_conn:
                    rts_conn.execute_command(f"TS.CREATE {timeseries_name} RETENTION 86400000 DUPLICATE_POLICY "
                                             f"{duplicate_policy} {label_pairs}")

        except Exception as e:
            logging.error(str(e))

        return timeseries_name

    def aggregate_one_timeseries(self, start_time: int, end_time: int, time_series_key: str, filter_labels: Dict,
                                 aggregations: Dict, show_labels=False):
        """
        This method is used for querying a particular timeseries key

        :param start_time: start time in milliseconds
        :param end_time: end time in milliseconds
        :param time_series_key: name of the timeseries
        :param filter_labels: key value pairs similar to tags in kairos
        :param aggregations: contains the type of aggregation as key and sampling as value
        :param show_labels: boolean true for getting the labels in the query result
        :return: query result containing datapoints and timestamp as list of lists
        """
        try:
            show_labels = "WITHLABELS" if show_labels else ''
            aggregations = self.time_converter(aggregations)
            aggregation = f"ALIGN START AGGREGATION {' '.join(f'{key} {value}' for key, value in aggregations.items())} " \
                          if aggregations else ""
            filters = f"FILTER {' '.join(f'{key} {value}' for key, value in filter_labels.items())}" \
                      if filter_labels else ""
            rts_query = f"TS.RANGE {time_series_key} {start_time} {end_time} {show_labels} " \
                        f"{aggregation} {filters}"
            with self.redis_client.redis_connect() as rts_conn:
                response = rts_conn.execute_command(rts_query)
            return response

        except Exception as e:
            logging.error(str(e))
            raise e

    def insert_bulk_datapoints(self, timeseries_key: str, data: Dict, duplicate_policy="Block"):
        """
        This method can be used to do bulk insertions of the datapoints to a specified timeseries key

        :param timeseries_key: name of the timeseries key
        :param data: dictionary containing the datapoints
        :param duplicate_policy: Option to choose in case of duplicate timestamps for a particular timeseries key
        :return: boolean indicating the query status
        """
        try:
            if duplicate_policy.lower() not in ["block", "first", "last", "min", "max", "sum"]:
                logging.error("Invalid Duplicate Policy Selected")
                return False
            labels_copy = copy.deepcopy(data)
            timeseries_name = self.create_new_timeseries(timeseries_name=timeseries_key,
                                                         labels=labels_copy, duplicate_policy=duplicate_policy)

            add_data = [f"{timeseries_name} {each_data[0]} {each_data[1]}" for each_data in data["datapoints"]]
            if add_data:
                final_query = "TS.MADD " + ", ".join(add_data)
                with self.redis_client.redis_connect() as rts_conn:
                    rts_conn.execute_command(final_query)
                    PostgresLogger().log_timeseries(timeseries_name=timeseries_name)
            return True

        except Exception as e:
            logging.error(str(e))
            raise e

    def execute_custom_rts_query(self, rts_query: str):
        """
        This method can be used when we want to execute our own custom query.

        :param rts_query: query to be executed
        :return: result of the query
        """
        try:
            with self.redis_client.redis_connect() as rts_conn:
                return rts_conn.execute_command(rts_query)
        except Exception as e:
            logging.error(str(e))
            raise e

    def delete_data(self, timeseries_key: str, from_timestamp="-", to_timestamp="+"):
        """
        This method can be used for deleting the data for the specified timeseries key.

        :param timeseries_key: name of the timeseries key
        :param from_timestamp: starting point of the range
        :param to_timestamp: ending point of the range
        :return: result specifying the query status
        """
        try:
            with self.redis_client.redis_connect() as rts_conn:
                return rts_conn.execute_command(f"TS.DEL {timeseries_key} {from_timestamp} {to_timestamp}")

        except Exception as e:
            logging.error(str(e))
            return False

    def aggregate_all_timeseries(self, start_time: int, end_time: int, filter_labels: Dict,
                                 aggregations: Dict, group_by_and_reduce: Dict, show_labels=False):
        """
        This method can be used to query on multiple timeseries keys. Atleast one label is required to filter in the
        filter_labels dictionary.

        :param start_time: start time in milliseconds
        :param end_time: end time in milliseconds
        :param filter_labels: Atleast one label name and value is required when querying multiple timeseries keys
        :param aggregations: Contains the type of aggregation as key and sampling as value
        :param group_by_and_reduce: group_by key for grouping labels and reduce key for identical timestamps
        :param show_labels: boolean true for getting the labels in the query result
        :return: response data as list of lists
        """
        try:
            group_by = f"GROUPBY {group_by_and_reduce['group_by']} REDUCE {group_by_and_reduce['reduce']}" \
                       if group_by_and_reduce and type(group_by_and_reduce) is dict else ""
            show_labels = "WITHLABELS" if show_labels else ''
            aggregations = self.time_converter(aggregations)
            aggregation = f"ALIGN START AGGREGATION {' '.join(f'{key} {value}' for key, value in aggregations.items())} " \
                          if aggregations else ""
            filters = f"FILTER {' '.join(f'{key}={value}' for key, value in filter_labels.items())}" \
                      if filter_labels else ""
            rts_query = f"TS.MRANGE {start_time} {end_time} {show_labels} {aggregation} {filters} {group_by}"
            with self.redis_client.redis_connect() as rts_conn:
                response = rts_conn.execute_command(rts_query)
            return response

        except Exception as e:
            logging.error(str(e))

    def insert_one_datapoint(self, timeseries_name: str, data: Dict, duplicate_policy="Block"):
        """
        This method can be used for inserting a single datapoint with respect to a timeseries key

        :param timeseries_name: Name of the timeseries
        :param data: dictionary containing the datapoints
        :param duplicate_policy: Option to choose in case of duplicate timestamps for a particular timeseries key
        :return: boolean indicating the query status
        """
        try:
            if duplicate_policy.lower() not in ["block", "first", "last", "min", "max", "sum"]:
                logging.error("Invalid Duplicate Policy Passed")
                return False
            labels_copy = copy.deepcopy(data)
            final_query, timeseries_key = \
                self.create_new_timeseries(timeseries_name=timeseries_name, labels=labels_copy,
                                           duplicate_policy=duplicate_policy, call_type="insert_one")
            final_query = final_query.replace("_timestamp", str(data["datapoints"][0][0])).\
                replace("_value", str(data["datapoints"][0][1]))

            with self.redis_client.redis_connect() as rts_conn:
                rts_conn.execute_command(final_query)
                PostgresLogger().log_timeseries(timeseries_name=timeseries_key)
            flag = True

        except Exception as e:
            logging.error(str(e))
            raise e

        return flag

    def alter_timeseries_key_configuration(self, timeseries_key: str, labels=None, duplicate_policy = None,
                                           retention_policy: int = 0):
        """
        This method is for altering the configurations like label values, retention and duplicate policies of a
        timeseries key

        :param timeseries_key: name of the timeseries to alter
        :param labels: dictionary containing updated labels
        :param duplicate_policy: policy for handling duplicate timestamps for a particular timeseries key
        :param retention_policy: policy for auto deleting the datapoints after a specific time period
        :return: boolean indicating the query status
        """

        try:
            labels = f" LABELS {' '.join(f'{key} {value}' for key, value in labels.items())} " \
                     if labels is not None else ""
            retention_policy = f"RETENTION {retention_policy}" if retention_policy else ""
            duplicate_policy = f"DUPLICATE_POLICY {duplicate_policy}" if duplicate_policy is not None else ""

            rts_query = f"TS.ALTER {timeseries_key} {retention_policy} {duplicate_policy} {labels}"
            with self.redis_client.redis_connect() as rts_conn:
                rts_conn.execute_command(rts_query)
                PostgresLogger().log_timeseries(timeseries_name=timeseries_key)
            return True

        except Exception as e:
            logging.error(str(e))
            return False

    @staticmethod
    def time_converter(time_dict: Dict):
        """
        This method takes the sampling time in units like days, months and returns the equivalent time in milliseconds.

        :param time_dict: dictionary containing aggregation as key and sampling as value
        :return: dictionary with equivalent sampling in milliseconds
        """
        time_dict = json.loads(json.dumps(time_dict).lower())
        for each_key in time_dict:
            try:
                if "milliseconds" in time_dict[each_key]:
                    time_dict[each_key] = int(time_dict[each_key].replace("milliseconds", ""))

                elif "seconds" in time_dict[each_key]:
                    time_dict[each_key] = int(time_dict[each_key].replace("seconds", "")) * 1000

                elif "minutes" in time_dict[each_key]:
                    time_dict[each_key] = int(time_dict[each_key].replace("minutes", "")) * 60 * 1000

                elif "hours" in time_dict[each_key]:
                    time_dict[each_key] = int(time_dict[each_key].replace("hours", "")) * 60 * 60 * 1000

                elif "days" in time_dict[each_key]:
                    time_dict[each_key] = int(time_dict[each_key].replace("days", "")) * 24 * 60 * 60 * 1000

                elif "weeks" in time_dict[each_key]:
                    time_dict[each_key] = int(time_dict[each_key].replace("weeks", "")) * 7 * 24 * 60 * 60 * 1000

                elif "months" in time_dict[each_key]:
                    time_dict[each_key] = int(time_dict[each_key].replace("months", "")) * 30 * 24 * 60 * 60 * 1000

                elif "years" in time_dict[each_key]:
                    time_dict[each_key] = int(time_dict[each_key].replace("years", "")) * 366 * 24 * 60 * 60 * 1000

            except Exception as e:
                logging.error(str(e))
                raise e
        return time_dict


class PostgresLogger:
    def __init__(self):
        self.sql_log = SqlDbReader()
        self.table_name = "redis_timeseries_logger"

    def log_timeseries(self, timeseries_name):
        """
        This method is used for selecting records from tables.

        :param timeseries_name: The select query to be executed
        :return: status: The status True on success and False on failure and the list of rows
        """
        try:
            check_query = f"""CREATE TABLE IF NOT EXISTS {self.table_name} (timeseries_name VARCHAR(200) PRIMARY KEY, 
                              last_updated_time BIGINT);"""
            current_timestamp = int(datetime.datetime.now().timestamp() * 1000)
            query = f"""INSERT INTO {self.table_name} VALUES ('{timeseries_name}', {current_timestamp})
                        ON CONFLICT (timeseries_name) DO UPDATE SET last_updated_time = {current_timestamp};"""
            with self.sql_log.dbconnect() as conn:
                cursor = conn.cursor()
                cursor.execute(check_query)
                cursor.execute(query)
                conn.commit()
        except Exception as e:
            logging.error(str(e))

    def redis_db_cleaner(self):
        """
        This method is used for inserting new records in tables.
        :return: status: The status True on success and False on failure
        """
        try:
            current_timestamp = int(datetime.datetime.now().timestamp() * 1000) - 60000
            query = f"""SELECT timeseries_name FROM {self.table_name} WHERE last_updated_time < {current_timestamp};"""
            with self.sql_log.sql_connect() as cursor:
                cursor.execute(query)
                timeseries_keys = cursor.fetchall()

            for each_key in timeseries_keys:
                try:
                    rts_deletion = f"TS.DEL {each_key[0]} - +"
                    RedisTimeSeries().execute_custom_rts_query(rts_query=rts_deletion)

                except Exception as e:
                    logging.error(str(e))
        except Exception as e:
            logging.error(str(e))