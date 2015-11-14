# Utilities

def year_release(b):
    import isodate

    r, y = b.identity.btime.upper().split('E')

    release = int(isodate.parse_duration(r).years)
    year = int(isodate.parse_date(y).year)

    return year, release