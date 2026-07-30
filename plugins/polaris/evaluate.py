import json
import math
import os
import sys
import time

from plugins.configuration import number


def clamp_replicas(value, min_replicas, max_replicas):
    return max(min(value, max_replicas), min_replicas)


def desired_replicas(
    current_replicas,
    response_time_millis,
    target_response_time_millis,
    min_replicas,
    max_replicas,
):
    desired = math.ceil(
        current_replicas * response_time_millis / target_response_time_millis
    )
    return clamp_replicas(desired, min_replicas, max_replicas)


def stabilized_target(
    current_replicas,
    candidate_replicas,
    last_scale_timestamp,
    current_timestamp,
    downscale_stabilization_seconds,
):
    if (
        candidate_replicas < current_replicas
        and current_timestamp - last_scale_timestamp
        < downscale_stabilization_seconds
    ):
        return current_replicas
    return candidate_replicas


def load_state(path):
    try:
        with open(path, encoding="utf-8") as state_file:
            value = json.load(state_file)
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"failed to read Polaris state: {error}") from error
    if not isinstance(value, dict):
        raise ValueError("Polaris state must be a JSON object")
    return value


def save_state(path, state):
    temporary_path = path + ".tmp"
    try:
        with open(temporary_path, "w", encoding="utf-8") as state_file:
            json.dump(state, state_file)
        os.replace(temporary_path, path)
    except OSError as error:
        raise ValueError(f"failed to write Polaris state: {error}") from error


def resource_key(resource):
    metadata = resource.get("metadata", {})
    namespace = metadata.get("namespace", "default")
    name = metadata.get("name")
    if not name:
        raise ValueError("autoscaler resource name is required")
    return f"{namespace}/{name}"


def current_replicas(resource):
    value = resource.get("spec", {}).get("replicas")
    if value is None:
        value = resource.get("status", {}).get("replicas")
    try:
        replicas = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("autoscaler resource replica count is required") from error
    if replicas < 0:
        raise ValueError("autoscaler resource replica count must be non-negative")
    return replicas


def evaluate(
    spec,
    target_response_time_millis,
    min_replicas,
    max_replicas,
    downscale_stabilization_seconds,
    state_path,
    current_timestamp=None,
):
    if len(spec.get("metrics", [])) != 1:
        raise ValueError("expected exactly one Polaris metric")
    try:
        metric_value = json.loads(spec["metrics"][0]["value"])
        resource = spec["resource"]
    except (KeyError, json.JSONDecodeError) as error:
        raise ValueError("invalid Polaris evaluation input") from error

    replicas = current_replicas(resource)
    state = load_state(state_path)
    key = resource_key(resource)
    resource_state = state.get(key, {})
    last_scale_timestamp = float(resource_state.get("last_scale_timestamp", 0.0))
    timestamp = time.time() if current_timestamp is None else current_timestamp

    if metric_value.get("available") is not True:
        target_replicas = replicas
    else:
        try:
            response_time = float(metric_value["response_time_millis"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("Polaris response-time metric is invalid") from error
        if not math.isfinite(response_time) or response_time < 0.0:
            raise ValueError("Polaris response-time metric must be finite and non-negative")
        candidate = desired_replicas(
            replicas,
            response_time,
            target_response_time_millis,
            min_replicas,
            max_replicas,
        )
        target_replicas = stabilized_target(
            replicas,
            candidate,
            last_scale_timestamp,
            timestamp,
            downscale_stabilization_seconds,
        )

    if target_replicas != replicas:
        state[key] = {"last_scale_timestamp": timestamp}
        save_state(state_path, state)

    print(json.dumps({"targetReplicas": target_replicas}))


def run(config):
    min_replicas = number(config, "minReplicas", int)
    max_replicas = number(config, "maxReplicas", int)
    target_response_time = number(config, "targetResponseTimeMillis")
    downscale_stabilization = number(
        config,
        "downscaleStabilizationSeconds",
    )
    if min_replicas < 1 or max_replicas < min_replicas:
        raise ValueError("replicas must satisfy 1 <= minReplicas <= maxReplicas")
    if target_response_time <= 0.0:
        raise ValueError("targetResponseTimeMillis must be greater than zero")
    if downscale_stabilization < 0.0:
        raise ValueError("downscaleStabilizationSeconds must be non-negative")

    try:
        spec = json.loads(sys.stdin.read())
    except json.JSONDecodeError as error:
        raise ValueError("failed to parse autoscaler request as JSON") from error

    evaluate(
        spec,
        target_response_time,
        min_replicas,
        max_replicas,
        downscale_stabilization,
        "/tmp/polaris-state.json",
    )
