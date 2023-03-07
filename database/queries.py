from datetime import datetime
from psycopg2 import Error, pool

USER =
PASSWORD =
HOST =
PORT = "5432"
DATABASE = "postgres"

connection_pool = pool.SimpleConnectionPool(minconn=1, maxconn=20, user=USER,
                                            password=PASSWORD,
                                            host=HOST,
                                            port=PORT,
                                            database=DATABASE
                                            )


async def get_status_in_range(start_time: datetime, end_time: datetime, store_ids: list[str]):
    try:
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        query = "SELECT status, timestamp_utc, store_id FROM store_status " \
                "WHERE store_id = ANY(%(store_ids)s) AND timestamp_utc BETWEEN %(start_time)s AND %(end_time)s " \
                "ORDER BY store_id, timestamp_utc ASC"
        params = {'start_time': start_time, 'end_time': end_time, 'store_ids': store_ids}

        cursor.execute(query, params)

        store_status_map: dict[str, list] = {}
        for store_id in store_ids:
            store_status_map.setdefault(store_id, [])
        rows = cursor.fetchall()
        for row in rows:
            store_id = row[2]
            timestamp_utc = row[1]
            status = row[0]
            store_status_map[store_id].append(
                [status, timestamp_utc]
            )
        return store_status_map

    except (Exception, Error) as error:
        print("Error while fetching store status in given time range", error)

    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


async def get_store_business_hours(store_ids: list[str], days: list[int]):
    try:
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        query = "SELECT start_time_local, end_time_local, day, store_id " \
                "FROM store_hours " \
                "WHERE store_id = ANY(%(store_ids)s) AND day = ANY(%(days)s)"
        params = {'store_ids': store_ids, 'days': days}

        cursor.execute(query, params)

        rows = cursor.fetchall()
        business_hours_map: dict[str, dict[int, list[list[str]]]] = {}
        for row in rows:
            start_time_local: str = row[0]
            end_time_local: str = row[1]
            day: int = row[2]
            store_id: str = row[3]
            if business_hours_map.get(store_id, None) is None:
                business_hours_map.setdefault(store_id, {})
                business_hours_map[store_id][day] = [[start_time_local, end_time_local]]
            else:
                if business_hours_map[store_id].get(day, None) is None:
                    business_hours_map[store_id][day] = [[start_time_local, end_time_local]]
                else:
                    business_hours_map[store_id][day].append([start_time_local, end_time_local])
        for store_id in store_ids:
            if business_hours_map.get(store_id, None) is None:
                business_hours_map.setdefault(store_id, {})
                for day in days:
                    business_hours_map[store_id][day] = [['00:00:00', '23:59:59']]
            else:
                for day in days:
                    if business_hours_map[store_id].get(day, None) is None:
                        business_hours_map[store_id][day] = [['00:00:00', '23:59:59']]
        # print(business_hours_map)
        return business_hours_map

    except (Exception, Error) as error:
        print("Error while fetching store buiness hours", error)

    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


async def get_store_time_zone(store_ids: list[str]):
    try:
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        query = "SELECT timezone_str, store_id " \
                "FROM store_timezones " \
                "WHERE store_id = ANY(%(store_ids)s)"
        params = {'store_ids': store_ids}

        cursor.execute(query, params)

        rows = cursor.fetchall()

        store_timezones_map: dict[str, str] = {}
        for store_id in store_ids:
            store_timezones_map[store_id] = 'America/Chicago'
        for row in rows:
            timezone_str = row[0]
            store_id = row[1]
            store_timezones_map[store_id] = timezone_str
        return store_timezones_map

    except (Exception, Error) as error:
        print("Error while fetching store timezone", error)

    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)


async def get_all_store_id(timestamp_limit: datetime):
    try:
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        query = "select store_id from store_status where timestamp_utc >= %(timestamp_limit)s " \
                "group by store_id;"
        params = {'timestamp_limit': timestamp_limit}

        cursor.execute(query, params)

        rows = cursor.fetchall()
        return rows

    except (Exception, Error) as error:
        print("Error while fetching all store_ids", error)

    finally:
        if connection:
            cursor.close()
            connection_pool.putconn(connection)
