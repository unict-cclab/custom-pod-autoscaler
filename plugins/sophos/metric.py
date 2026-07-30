import argparse
import json
import logging
import sys

from plugins import prometheus
from plugins.configuration import boolean, number, optional, required

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_label_value(labels, key):
    if key not in labels:
        logging.error("No '%s' label on resource being managed", key)
        sys.exit(1)
    return labels[key]


def prom_query(prometheus_url, query):
    return prometheus.query(prometheus_url, query)


def prom_scalar(prometheus_url, query):
    return prometheus.scalar(prometheus_url, query)


def app_label_response_time(
    prometheus_url,
    app,
    app_label,
    reporter,
    target_percentage,
    time_range,
):
    query = f"""
        histogram_quantile(
            {target_percentage},
            sum by (le) (
                rate(
                    istio_request_duration_milliseconds_bucket{{
                        {app_label}="{app}",
                        reporter="{reporter}",
                        request_protocol!="grpc",
                        response_code!~"5.."
                    }}[{time_range}]
                )
                or
                rate(
                    istio_request_duration_milliseconds_bucket{{
                        {app_label}="{app}",
                        reporter="{reporter}",
                        request_protocol="grpc",
                        grpc_response_status="0"
                    }}[{time_range}]
                )
            )
        )
    """
    return prom_scalar(prometheus_url, query)


def response_time_error(
    prometheus_url,
    app,
    target_response_time,
    target_percentage,
    time_range,
    exclude_outbound_response_time=False,
):
    inbound_response_time = app_label_response_time(
        prometheus_url,
        app,
        "destination_app",
        "destination",
        target_percentage,
        time_range,
    )
    outbound_response_time = app_label_response_time(
        prometheus_url,
        app,
        "source_app",
        "source",
        target_percentage,
        time_range,
    )
    service_response_time = max(0.0, inbound_response_time)
    if not exclude_outbound_response_time:
        service_response_time = max(
            0.0,
            inbound_response_time - outbound_response_time,
        )
    error = (service_response_time - target_response_time) / target_response_time
    return error, inbound_response_time, outbound_response_time, service_response_time


def metrics(
    spec,
    prometheus_url,
    target_response_time,
    target_percentage,
    time_range,
    min_rps_for_error,
    exclude_outbound_response_time=False,
):
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
    rps = prom_scalar(prometheus_url, query)

    (
        err,
        inbound_response_time,
        outbound_response_time,
        service_response_time,
    ) = response_time_error(
        prometheus_url,
        app,
        target_response_time,
        target_percentage,
        time_range,
        exclude_outbound_response_time,
    )
    if rps < min_rps_for_error:
        err = min(0.0, err)

    query = f"""
        avg_over_time(
        kube_deployment_status_replicas_ready{{
            deployment="{app}"
        }}[{time_range}]
        )
    """
    avg_ready_replicas = prom_scalar(prometheus_url, query)

    print(
        json.dumps(
            {
                "rps": rps,
                "err": err,
                "avg_ready_replicas": avg_ready_replicas,
                "inbound_response_time": inbound_response_time,
                "outbound_response_time": outbound_response_time,
                "service_response_time": service_response_time,
            }
        )
    )


def run(config):
    target_response_time = number(config, "targetResponseTimeMillis")
    target_percentage = number(config, "targetPercentage")
    if target_response_time <= 0.0:
        raise ValueError("targetResponseTimeMillis must be greater than zero")
    if not 0.0 < target_percentage < 1.0:
        raise ValueError("targetPercentage must be between zero and one")

    try:
        spec = json.loads(sys.stdin.read())
    except json.JSONDecodeError as error:
        raise ValueError("failed to parse autoscaler request as JSON") from error

    metrics(
        spec,
        required(config, "prometheusURL"),
        target_response_time,
        target_percentage,
        required(config, "timeRange"),
        float(optional(config, "minGoodRPSForError", 5.0)),
        boolean(config, "excludeOutboundResponseTime"),
    )


def legacy_main():
    parser = argparse.ArgumentParser(
        description="Compute response-time controller metrics."
    )
    parser.add_argument("--prometheus_url", required=True)
    parser.add_argument("--target_response_time", required=True, type=float)
    parser.add_argument("--target_percentage", required=True, type=float)
    parser.add_argument("--time_range", required=True)
    parser.add_argument(
        "--exclude_outbound_response_time",
        choices=("true", "false"),
        default="false",
    )
    parser.add_argument("--min_good_rps_for_error", type=float, default=5.0)
    args = parser.parse_args()
    if args.target_response_time <= 0.0:
        parser.error("--target_response_time must be greater than zero")
    if not 0.0 < args.target_percentage < 1.0:
        parser.error("--target_percentage must be between zero and one")
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
        args.exclude_outbound_response_time == "true",
    )
