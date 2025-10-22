import re


def calc_resolution(res: str | float | int) -> float:
    """
    Extract nominal resolution from file.nominal_resolution and return in degrees.
    Supports 'xx km', 'x x degree', or 'x degree'. Returns large value if unknown.
    Handles both string and numeric input.
    """
    if isinstance(res, (float, int)):
        return float(res)
    if not res:
        return 9999.0
    res = str(res).lower().replace(" ", "")
    if m := re.match(r"([\d.]+)km", res):
        return float(m.group(1)) / 111.0
    if m := re.match(r"([\d.]+)x([\d.]+)degree", res):
        return (float(m.group(1)) + float(m.group(2))) / 2.0
    if m := re.match(r"([\d.]+)degree", res):
        return float(m.group(1))
    return 9999.0