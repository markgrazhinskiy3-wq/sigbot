from datetime import date, timedelta


def parse_custom_range(text: str) -> tuple[date, date]:
    """
    Parse a custom date range from user input.
    Expected format: DD.MM.YYYY - DD.MM.YYYY
    """
    parts = [p.strip() for p in text.split("-")]
    if len(parts) != 2:
        raise ValueError(
            "Неверный формат. Используйте: DD.MM.YYYY - DD.MM.YYYY"
        )

    date_from = _parse_date(parts[0])
    date_to = _parse_date(parts[1])

    if date_from > date_to:
        raise ValueError("Начальная дата не может быть позже конечной.")

    return date_from, date_to


def _parse_date(s: str) -> date:
    s = s.strip()
    try:
        day, month, year = s.split(".")
        return date(int(year), int(month), int(day))
    except Exception:
        raise ValueError(
            f"Не удалось распознать дату: '{s}'. Используйте формат DD.MM.YYYY"
        )


def get_predefined_range(period: str) -> tuple[date, date]:
    """
    Return (date_from, date_to) for a named period.
    """
    today = date.today()

    match period:
        case "today":
            return today, today
        case "yesterday":
            y = today - timedelta(days=1)
            return y, y
        case "last_7":
            return today - timedelta(days=6), today
        case "last_30":
            return today - timedelta(days=29), today
        case _:
            raise ValueError(f"Unknown period: {period}")


def fmt(d: date) -> str:
    """Format date as DD.MM.YYYY."""
    return d.strftime("%d.%m.%Y")
