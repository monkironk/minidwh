
#alle Fakten Crime haben/bekommen Wetter ID
from db import get_conn


def fill_weather_ids():
    """
    Setzt weatherID = dateID für alle Datensätze in crimes_fakten,s
    bei denen weatherID noch NULL ist.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE crimes_fakten
                SET weatherID = dateID
                WHERE weatherID IS NULL;
            """)
