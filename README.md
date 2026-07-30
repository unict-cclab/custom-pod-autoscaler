# Pluggable Custom Pod Autoscaler

Metric and evaluation plugins used by the Custom Pod Autoscaler operator. The
image dispatches both phases through `run_plugin.py`; `plugin` selects the
implementation and defaults to `sophos` for compatibility.

## Plugin contract

A runtime plugin is a Python package below `plugins/<name>/` with `metric.py`
and `evaluate.py` modules. Each module exports a `run(config)` function, reads
the operator request from stdin, validates the supplied camelCase configuration
object, and writes the operator response JSON to stdout. Adding `polaris` or
`smarthpa` at runtime requires:

```text
plugins/
  polaris/
    __init__.py
    metric.py      # run(config)
    evaluate.py    # run(config)
```

The shared `config.yaml` and Docker entrypoint need no changes. Invalid or
unknown plugin names fail before an implementation is invoked. The root
`metric.py` and `evaluate.py` files are compatibility shims that delegate to
`plugins/sophos`; they contain no autoscaling implementation.

To make a new plugin selectable from an experiment, its application renderer
must additionally define and validate the plugin's public camelCase schema and
render it into the CPA configuration. The experiment executor remains
unchanged.

The renderer writes the camelCase configuration to a JSON file in a ConfigMap,
which is mounted into each autoscaler container. The dispatcher reads that file
and passes the `config` object directly to the plugin. Plugin settings are not
transported as environment variables and there is no alias layer.

## Polaris plugin

The `polaris` plugin implements the latency-SLO horizontal scaling strategy
from *High-Level Metrics for Service Level Objective-aware Autoscaling in
Polaris: a Performance Evaluation*. For current replicas `R`, observed inbound
latency `L`, and target latency `T`, it evaluates:

```text
targetReplicas = ceil(R * L / T)
```

The result is clamped to the configured replica bounds. Upscaling is immediate;
downscaling is blocked for `downscaleStabilizationSeconds` after the last
scaling decision. Missing Prometheus samples hold the current replica count.
The plugin stores only its scaling timestamp in
`/tmp/polaris-state.json`, because each CPA runs per resource.

The metric is the successful HTTP/gRPC inbound Istio latency percentile for
the target service. `targetPercentage` and `timeRange` default to `0.95` and
`1m`. Outbound latency subtraction is not part of the Polaris contract.

[Polaris publication](https://dsg.tuwien.ac.at/~sd/papers/ICFEC_2022_T_Pusztai_High_Level.pdf)

## Sophos plugin

## Metric parameters

| Parameter | Description |
| --- | --- |
| `prometheusURL` | Prometheus query endpoint |
| `targetResponseTimeMillis` | Target service response time in milliseconds |
| `targetPercentage` | Response-time percentile used by the controller |
| `timeRange` | Prometheus query range |
| `excludeOutboundResponseTime` | Whether to ignore outbound response time and use inbound response time directly; defaults to `false` |

By default, the metric script retains the original behavior and estimates a
service's own response time as the difference between its inbound and outbound
response-time percentiles:

```text
service_response_time = max(0, inbound_response_time - outbound_response_time)
err = (service_response_time - target_response_time) / target_response_time
```

Set `excludeOutboundResponseTime=true` to exclude the outbound percentile
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
contradictory observation moves the opposite bound by the configured margin.
The safe bound caps every PID upscale; during a positive error, the cap still
allows at least one additional replica.

## Evaluation parameters

| Parameter | Description |
| --- | --- |
| `redisHost` | Redis hostname |
| `kp`, `ki`, `kd` | PID coefficients |
| `minReplicas` | Minimum replicas |
| `maxReplicas` | Maximum replicas |
| `downscaleStabilizationSeconds` | Downscale stabilization period |
| `marginRatio` | Fractional margin between RPS-per-replica bounds in `(0, 1)`; defaults to `0.1` |

[`config.yaml`](config.yaml) invokes the shared plugin dispatcher. Both plugin
phases read the autoscaler request as JSON from stdin and write JSON to stdout.
