import logging
import math
import sys

import requests


def query(prometheus_url, promql):
    try:
        response = requests.get(
            prometheus_url,
            params={"query": promql},
            timeout=10,
        )
        response.raise_for_status()
        return response.json().get("data", {}).get("result", [])
    except (requests.RequestException, ValueError) as error:
        logging.error("Failed to query Prometheus: %s", error)
        sys.exit(1)


def scalar(prometheus_url, promql):
    results = query(prometheus_url, promql)
    value = float(results[0]["value"][1]) if results else 0.0
    if not math.isfinite(value):
        return 0.0
    return value
