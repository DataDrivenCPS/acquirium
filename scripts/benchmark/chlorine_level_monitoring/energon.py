#22 lines

import time
import requests

ALERT_URL = "http://host.docker.internal:10000/alerts"
THRESHOLD = 75.0

def fetch_latest_chlorine():
    query = """
SELECT ChlorinationBasin(B) * Data(B)
FROM Building B
WHERE B.Source = 'Benicia'
FILTER Data.Unit IN ('MilliGM-PER-L')
"""
    df = fetch(query, latest=True, cast_value="float")  # illustrative API
    return df

def post_alert(message: dict) -> None:
    requests.post(ALERT_URL, json=message, timeout=5)

def main() -> None:
    while True:
        df = fetch_latest_chlorine()

        if df is None or df.is_empty() or df.shape[0] == 0:
            post_alert({"text": "No data available for chlorine level.", "severity": "LOW", "data": {}})
        else:
            ts = df[0, 0]
            val = float(df[0, 1])

            if val > THRESHOLD:
                post_alert({"text": "Chlorine level exceeds safe threshold.", "severity": "HIGH",
                            "data": {"chlorine_level": val, "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts)}})
            else:
                post_alert({"text": "Chlorine level is within safe limits.", "severity": "NORMAL",
                            "data": {"chlorine_level": val, "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts)}})

        time.sleep(10)

if __name__ == "__main__":
    main()
