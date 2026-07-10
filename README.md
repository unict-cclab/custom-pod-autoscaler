# Custom Pod Autoscaler

Metric and evaluation scripts used by the Custom Pod Autoscaler operator.

## Metric parameters

| Parameter | Description |
| --- | --- |
| `PROMETHEUS_URL` | Prometheus query endpoint |
| `TARGET_RESPONSE_TIME` | Target response time in milliseconds |
| `TARGET_PERCENTAGE` | Target request percentile |
| `TIME_RANGE` | Prometheus query range |

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
