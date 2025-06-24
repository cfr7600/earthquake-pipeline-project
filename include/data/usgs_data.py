def fetchData():
    import logging
    import requests
    import json
    from datetime import date

    logging.basicConfig(level=logging.INFO)

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    base_params = {
        "format": "geojson",
        "minmagnitude": 4,
        "orderby": "time",
        "limit": 20000
    }

    current_year = date.today().year
    years_back = 10
    all_features = []

    try:
        for year in range(current_year, current_year - years_back, -1):
            params = base_params.copy()
            params["starttime"] = f"{year}-01-01"
            params["endtime"] = f"{year}-12-31"

            response = requests.get(url, params=params)
            if response.status_code == 200:
                features = response.json().get("features", [])
                all_features.extend(features)
            else:
                logging.warning(f"Non-200 response for year {year}: {response.text}")

        with open("/usr/local/airflow/include/data/earthquake-10-year-data.jsonl", "w") as f:
            for feature in all_features:
                f.write(json.dumps(feature) + "\n")

        logging.info(f"Saved {len(all_features)} features to local file")
    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        raise

