import argparse
import json
import sys
import math
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def get_label_value(labels, key):
    if key not in labels:
        logging.error(f"No '{key}' label on resource being managed")
        sys.exit(1)
    return labels[key]

def prom_query(prometheus_url, query):
    try:
        response = requests.get(prometheus_url, params={"query": query}, timeout=10)
        response.raise_for_status()
        return response.json().get("data", {}).get("result", [])
    except (requests.RequestException, ValueError, KeyError) as e:
        logging.error(f"Failed to query Prometheus: {e}")
        sys.exit(1)


def prom_scalar(prometheus_url, query):
    results = prom_query(prometheus_url, query)
    value = float(results[0]["value"][1]) if results else 0.0
    if math.isnan(value):
        return 0.0
    return value


def app_label_slo_error(prometheus_url, app, app_label, reporter, target_response_time, target_percentage, time_range):
    http_good_under_target_query = f"""
        sum(
            rate(
                istio_request_duration_milliseconds_bucket{{
                    connection_security_policy="mutual_tls",
                    {app_label}="{app}",
                    reporter="{reporter}",
                    request_protocol!="grpc",
                    response_code!~"5..",
                    le="{target_response_time}"
                }}[{time_range}]
            )
        )
    """
    http_good_total_query = f"""
        sum(
            rate(
                istio_request_duration_milliseconds_count{{
                    connection_security_policy="mutual_tls",
                    {app_label}="{app}",
                    reporter="{reporter}",
                    request_protocol!="grpc",
                    response_code!~"5.."
                }}[{time_range}]
            )
        )
    """
    grpc_good_under_target_query = f"""
        sum(
            rate(
                istio_request_duration_milliseconds_bucket{{
                    connection_security_policy="mutual_tls",
                    {app_label}="{app}",
                    reporter="{reporter}",
                    request_protocol="grpc",
                    grpc_response_status="0",
                    le="{target_response_time}"
                }}[{time_range}]
            )
        )
    """
    grpc_good_total_query = f"""
        sum(
            rate(
                istio_request_duration_milliseconds_count{{
                    connection_security_policy="mutual_tls",
                    {app_label}="{app}",
                    reporter="{reporter}",
                    request_protocol="grpc",
                    grpc_response_status="0"
                }}[{time_range}]
            )
        )
    """

    good_under_target = (
        prom_scalar(prometheus_url, http_good_under_target_query)
        + prom_scalar(prometheus_url, grpc_good_under_target_query)
    )
    good_total = (
        prom_scalar(prometheus_url, http_good_total_query)
        + prom_scalar(prometheus_url, grpc_good_total_query)
    )
    if good_total <= 0.0:
        return 0.0

    good_fraction = good_under_target / good_total
    if math.isnan(good_fraction):
        return 0.0
    return target_percentage - good_fraction


def slo_error(prometheus_url, app, target_response_time, target_percentage, time_range):
    inbound_err = app_label_slo_error(
        prometheus_url,
        app,
        "destination_app",
        "destination",
        target_response_time,
        target_percentage,
        time_range,
    )
    outbound_err = app_label_slo_error(
        prometheus_url,
        app,
        "source_app",
        "source",
        target_response_time,
        target_percentage,
        time_range,
    )
    return inbound_err - max(0.0, outbound_err)


def metrics(spec, prometheus_url, target_response_time, target_percentage, time_range, min_good_rps_for_error):
    try:
        labels = spec["resource"]["metadata"]["labels"]
    except KeyError:
        logging.error("Invalid spec format: missing metadata.labels")
        sys.exit(1)

    app = get_label_value(labels, "app")

    query = f"""
        sum by (destination_app) (
            rate(istio_requests_total{{destination_app="{app}", reporter="destination"}}[{time_range}])
        )
    """

    results = prom_query(prometheus_url, query)
    rps = float(results[0]["value"][1]) if results else 0.0
    if math.isnan(rps):
        rps = 0.0

    err = slo_error(prometheus_url, app, target_response_time, target_percentage, time_range)
    if rps < min_good_rps_for_error:
        err = min(0.0, err)

    query = f"""
        avg_over_time(
        kube_deployment_status_replicas{{
            deployment="{app}"
        }}[{time_range}]
        )
    """

    results = prom_query(prometheus_url, query)
    avg_replicas = float(results[0]["value"][1]) if results else 0.0
    if math.isnan(avg_replicas):
        avg_replicas = 0.0

    output = {
        "rps": rps,
        "error": err,
        "avg_replicas": avg_replicas
    }

    print(json.dumps(output))


def main():
    parser = argparse.ArgumentParser(description="Compute SLO error metrics.")
    parser.add_argument("--prometheus_url", required=True, help="Prometheus url.")
    parser.add_argument("--target_response_time", required=True, type=float, help="Target response time.")
    parser.add_argument("--target_percentage", required=True, type=float, help="Target response time.")
    parser.add_argument("--time_range", required=True, help="Prometheus query range, e.g., '5m'.")
    parser.add_argument(
        "--min_good_rps_for_error",
        type=float,
        default=5.0,
        help="Minimum successful request rate required before computing SLO error.",
    )
    args = parser.parse_args()

    try:
        spec = json.loads(sys.stdin.read())
    except json.JSONDecodeError:
        logging.error("Failed to parse JSON from stdin")
        sys.exit(1)

    metrics(
        spec,
        args.prometheus_url,
        args.target_response_time,
        args.target_percentage,
        args.time_range,
        args.min_good_rps_for_error,
    )

if __name__ == "__main__":
    main()
