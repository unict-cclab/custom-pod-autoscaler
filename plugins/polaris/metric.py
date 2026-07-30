import json
import math
import sys

from plugins import prometheus
from plugins.configuration import optional, required


def latency_query(app, target_percentage, time_range):
    return f"""
        histogram_quantile(
            {target_percentage},
            sum by (le) (
                rate(
                    istio_request_duration_milliseconds_bucket{{
                        destination_app="{app}",
                        reporter="destination",
                        request_protocol!="grpc",
                        response_code!~"5.."
                    }}[{time_range}]
                )
                or
                rate(
                    istio_request_duration_milliseconds_bucket{{
                        destination_app="{app}",
                        reporter="destination",
                        request_protocol="grpc",
                        grpc_response_status="0"
                    }}[{time_range}]
                )
            )
        )
    """


def response_time_metric(prometheus_url, app, target_percentage, time_range):
    results = prometheus.query(
        prometheus_url,
        latency_query(app, target_percentage, time_range),
    )
    if not results:
        return {"available": False, "response_time_millis": 0.0}
    try:
        value = float(results[0]["value"][1])
    except (KeyError, IndexError, TypeError, ValueError) as error:
        raise ValueError("Prometheus returned an invalid latency sample") from error
    if not math.isfinite(value) or value < 0.0:
        return {"available": False, "response_time_millis": 0.0}
    return {"available": True, "response_time_millis": value}


def run(config):
    target_percentage = float(optional(config, "targetPercentage", 0.95))
    time_range = optional(config, "timeRange", "1m")
    if not 0.0 < target_percentage < 1.0:
        raise ValueError("targetPercentage must be between zero and one")

    try:
        spec = json.loads(sys.stdin.read())
        app = spec["resource"]["metadata"]["labels"]["app"]
    except json.JSONDecodeError as error:
        raise ValueError("failed to parse autoscaler request as JSON") from error
    except KeyError as error:
        raise ValueError("autoscaler resource must have an app label") from error

    metric = response_time_metric(
        required(config, "prometheusURL"),
        app,
        target_percentage,
        time_range,
    )
    print(json.dumps(metric))
