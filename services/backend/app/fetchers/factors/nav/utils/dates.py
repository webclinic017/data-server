def is_weekend(day):
    return day.weekday() in [5, 6]


def business_days(calendar, day):
    """Returns an integer indicating the xth number day in the month.

    :param calendar: list - Business days calendar
    :param day: date - Date to be checked
    :return: int - xth day in the month
    """
    first = day.replace(day=1).strftime("%Y-%m-%d")
    days = calendar[calendar >= first]
    return days.get_loc(day) + 1
