# Custom Pod Autoscaler

Metric and evaluation scripts used by the Custom Pod Autoscaler operator.

## Metric parameters

| Parameter | Description |
| --- | --- |
| `PROMETHEUS_URL` | Prometheus query endpoint |
| `TARGET_RESPONSE_TIME` | Target service response time in milliseconds |
| `TARGET_PERCENTAGE` | Response-time percentile used by the controller |
| `TIME_RANGE` | Prometheus query range |
| `EXCLUDE_OUTBOUND_RESPONSE_TIME` | Whether to ignore outbound response time and use inbound response time directly; defaults to `false` |

By default, the metric script retains the original behavior and estimates a
service's own response time as the difference between its inbound and outbound
response-time percentiles:

```text
service_response_time = max(0, inbound_response_time - outbound_response_time)
error = (service_response_time - target_response_time) / target_response_time
```

Set `EXCLUDE_OUTBOUND_RESPONSE_TIME=true` to exclude the outbound percentile
from the calculation entirely:

```text
service_response_time = max(0, inbound_response_time)
```

Only successful HTTP and gRPC requests contribute to the response-time
histograms. Failed requests are excluded because availability is outside the
controller's response-time objective.

Upscaling is driven only by the PID response-time error. The evaluator also
learns the maximum RPS per replica observed without error and the minimum RPS
per replica observed with error. The unsafe bound limits downscaling; a later
safe observation above it invalidates the stale bound.

## Evaluation parameters

| Parameter | Description |
| --- | --- |
| `REDIS_HOST` | Redis hostname |
| `KP`, `KI`, `KD` | PID coefficients |
| `MIN_REPLICAS` | Minimum replicas |
| `MAX_REPLICAS` | Maximum replicas |
| `DOWNSCALE_STABILIZATION` | Downscale stabilization period |

[`config.yaml`](config.yaml) maps these variables to `metric.py` and
`evaluate.py`. Both scripts read the autoscaler request as JSON from stdin and
write JSON to stdout.
