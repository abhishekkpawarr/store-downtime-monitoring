import asyncio
import random
import string
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
from starlette.responses import StreamingResponse

from database import queries

report_status: dict[str, str] = {}
reports: dict[str, list[tuple]] = {}
polling_interval = timedelta(hours=1)


async def get_report(report_id: str):
    if report_status.get(report_id, "NOT_FOUND") != "COMPLETED":
        return report_status.get(report_id, "NOT_FOUND")
    else:
        df = pd.DataFrame(
            reports[report_id],
            columns=["store_id", "uptime_last_hour", "uptime_last_day", "uptime_last_week", "downtime_last_hour",
                     "downtime_last_day", "downtime_last_week"]
        )
        return StreamingResponse(
            iter([df.to_csv(index=False)]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=report_{report_id}.csv"}
        )


async def trigger_report():
    # database.queries.open_connection()
    report_id = ''.join(random.choices(string.ascii_lowercase, k=5))
    report_status[report_id] = "RUNNING"
    task = asyncio.create_task(compute_report(report_id))
    # database.queries.close_connection()
    response = {"report_id": report_id}
    return response


async def compute_report(report_id: str):
    # curr_datetime = datetime.datetime.now()
    curr_datetime = datetime.strptime('2023-01-25 18:13:22', '%Y-%m-%d %H:%M:%S')

    all_stores = await queries.get_all_store_id(curr_datetime - timedelta(days=7))
    overall_report = []
    batch_size = 500
    batched_stores = []
    batch_counter = 0
    curr_batch_stores = []
    for stores in all_stores:
        curr_batch_stores.append(stores[0])
        batch_counter += 1
        if batch_counter >= batch_size:
            batch_counter = 0
            batched_stores.append(curr_batch_stores)
            curr_batch_stores = []
    if len(curr_batch_stores) > 0:
        batched_stores.append(curr_batch_stores)
    for index in range(len(batched_stores)):
        for report in await calculate_store__batch_report(batched_stores[index], curr_datetime, index):
            overall_report.append(report)
    report_status[report_id] = "COMPLETED"
    reports[report_id] = overall_report
    return overall_report


async def calculate_store__batch_report(store_ids: list[str], curr_datetime: datetime, index: int):
    last_7_days_business_hours = await get_last_n_days_utc_business_hours(store_ids, curr_datetime, 7)
    last_7_days_status_values = await queries.get_status_in_range(
        curr_datetime - timedelta(days=7) - polling_interval, curr_datetime, store_ids)
    return await asyncio.gather(
        *[calculate_store_report(store_id, curr_datetime, index,
                                 last_7_days_business_hours[store_id],
                                 last_7_days_status_values[store_id]
                                 ) for store_id in store_ids]
    )


async def calculate_store_report(store_id: str, curr_datetime: datetime, index: int, last_7_days_business_hours: list,
                                 last_7_days_status_values: list):
    uptime_last_week = 0.00
    downtime_last_week = 0.00
    uptime_last_day = 0.00
    downtime_last_day = 0.00
    uptime_last_hour = 0.00
    downtime_last_hour = 0.00

    print(store_id, index)

    for hours in get_business_hours_last_week(store_id, curr_datetime, last_7_days_business_hours):
        uptime_in_minutes = get_uptime_minutes(hours[0], hours[1], last_7_days_status_values)
        uptime_last_week += uptime_in_minutes
        downtime_last_week += float((hours[1] - hours[0]).seconds / 60) - uptime_in_minutes
    uptime_last_week /= 60
    downtime_last_week /= 60

    for hours in get_business_hours_last_day(store_id, curr_datetime, last_7_days_business_hours):
        uptime_in_minutes = get_uptime_minutes(hours[0], hours[1], last_7_days_status_values)
        uptime_last_day += uptime_in_minutes
        downtime_last_day += float((hours[1] - hours[0]).seconds / 60) - uptime_in_minutes
    uptime_last_day /= 60
    downtime_last_day /= 60

    for hours in get_business_hours_last_hour(store_id, curr_datetime, last_7_days_business_hours):
        uptime_in_minutes = get_uptime_minutes(hours[0], hours[1], last_7_days_status_values)
        uptime_last_hour += uptime_in_minutes
        downtime_last_hour += float((hours[1] - hours[0]).seconds / 60) - uptime_in_minutes

    print(str([store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day,
               downtime_last_week]))
    return (store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day,
            downtime_last_week)


def get_business_hours_last_week(store_id: str, curr_datetime: datetime,
                                 last_7_days_business_hours: list[list[datetime]]):
    # print(f"last_week {curr_datetime}")
    utc_business_hours = last_7_days_business_hours
    formatted_utc_business_hours = []
    # print(str(utc_business_hours))
    for hours in utc_business_hours:
        if hours[0] < curr_datetime < hours[1]:
            formatted_utc_business_hours.append(
                [hours[0], curr_datetime]
            )
        elif hours[1] < curr_datetime:
            formatted_utc_business_hours.append(
                [hours[0], hours[1]]
            )
    # print(str(formatted_utc_business_hours))
    return formatted_utc_business_hours


def get_business_hours_last_day(store_id: str, curr_datetime: datetime,
                                last_7_days_business_hours: list[list[datetime]]):
    # print(f"last_day {curr_datetime}")
    utc_business_hours = last_7_days_business_hours
    formatted_utc_business_hours = []
    # print(str(utc_business_hours))
    for hours in utc_business_hours:
        if hours[0] < curr_datetime - timedelta(days=1) < hours[1] < curr_datetime:
            formatted_utc_business_hours.append(
                [curr_datetime - timedelta(days=1), hours[1]]
            )
        elif hours[0] < curr_datetime - timedelta(days=1) < curr_datetime < hours[1]:
            formatted_utc_business_hours.append(
                [curr_datetime - curr_datetime]
            )
        elif curr_datetime - timedelta(days=1) < hours[0] < curr_datetime < hours[1]:
            formatted_utc_business_hours.append(
                [hours[0], curr_datetime]
            )
        elif curr_datetime - timedelta(days=1) < hours[0] < hours[1] < curr_datetime:
            formatted_utc_business_hours.append(
                [hours[0], hours[1]]
            )
    # print(str(formatted_utc_business_hours))
    return formatted_utc_business_hours


def get_business_hours_last_hour(store_id: str, curr_datetime: datetime,
                                 last_7_days_business_hours: list[list[datetime]]):
    # print(f"last_hour {curr_datetime}")
    utc_business_hours = last_7_days_business_hours
    formatted_utc_business_hours = []
    # print(str(utc_business_hours))
    for hours in utc_business_hours:
        if curr_datetime - timedelta(hours=1) < hours[0] < curr_datetime < hours[1]:
            formatted_utc_business_hours.append(
                [hours[0], curr_datetime]
            )
        elif hours[0] < curr_datetime - timedelta(hours=1) < curr_datetime < hours[1]:
            formatted_utc_business_hours.append(
                [curr_datetime - timedelta(hours=1), curr_datetime]
            )
        elif hours[0] < curr_datetime - timedelta(hours=1) < hours[1] < curr_datetime:
            formatted_utc_business_hours.append(
                [curr_datetime - timedelta(hours=1), hours[1]]
            )
    # print(str(formatted_utc_business_hours))
    return formatted_utc_business_hours


async def get_last_n_days_utc_business_hours(store_ids: list[str], curr_datetime: datetime, n: int):
    store_timezones_map = await queries.get_store_time_zone(store_ids)
    # print(store_timezone)
    week_business_hours = await queries.get_store_business_hours(store_ids, [0, 1, 2, 3, 4, 5, 6])
    utc_business_hours_map: dict[str, list[list[datetime]]] = {}
    for store_id in store_ids:
        utc_business_hours = []
        for diff in range(n):
            iter_datetime = curr_datetime - timedelta(days=diff)
            business_hours = week_business_hours[store_id][iter_datetime.weekday()]
            for hours in business_hours:
                start_time_local = datetime.strptime(hours[0], '%H:%M:%S')
                end_time_local = datetime.strptime(hours[1], '%H:%M:%S')
                utc_business_hours.append(
                    [
                        iter_datetime.replace(
                            hour=start_time_local.hour, minute=start_time_local.minute, second=start_time_local.second,
                            tzinfo=ZoneInfo(key=store_timezones_map[store_id])
                        ).astimezone(tz=timezone.utc).replace(tzinfo=None),
                        iter_datetime.replace(
                            hour=end_time_local.hour, minute=end_time_local.minute, second=end_time_local.second,
                            tzinfo=ZoneInfo(key=store_timezones_map[store_id])
                        ).astimezone(tz=timezone.utc).replace(tzinfo=None)
                    ]
                )
        utc_business_hours_map.setdefault(store_id, utc_business_hours)
    return utc_business_hours_map


def get_uptime_minutes(start_time: datetime, end_time: datetime, status_in_range: list[list]):
    formatted_polling_status = []
    for status in status_in_range:
        if start_time < status[1] < status[1] + polling_interval < end_time:
            formatted_polling_status.append(
                [status[1],
                 status[1] + polling_interval,
                 status[0]]
            )
        elif status[1] < start_time < status[1] + polling_interval < end_time:
            formatted_polling_status.append(
                [start_time,
                 status[1] + polling_interval,
                 status[0]]
            )
        elif start_time < status[1] < end_time < status[1] + polling_interval:
            formatted_polling_status.append(
                [status[1],
                 end_time,
                 status[0]]
            )

    # print(str(formatted_polling_status))
    if len(formatted_polling_status) < 1:
        return 0.00

    curr_end = formatted_polling_status[0][0]
    for i in range(len(formatted_polling_status)):
        if formatted_polling_status[i][0] < curr_end:
            formatted_polling_status[i - 1][1] = formatted_polling_status[i][0]
            curr_end = formatted_polling_status[i][1]
        else:
            curr_end = formatted_polling_status[i][1]

    # print(str(formatted_polling_status))

    uptime = float(0.00)
    for status in formatted_polling_status:
        if status[2] == 'active':
            uptime += float((status[1] - status[0]).seconds) / 60
    return uptime
