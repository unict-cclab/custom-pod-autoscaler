import argparse
import json
import logging
import math
import sys
import time

import redis

from plugins.configuration import number, optional, required

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_label_value(labels, key):
    if key not in labels:
        logging.error("No '%s' label on resource being managed", key)
        sys.exit(1)
    return labels[key]


def redis_key(group, app, suffix):
    return f"{group}_{app}_{suffix}"


def clamp_replicas(value, min_replicas, max_replicas):
    return max(min(value, max_replicas), min_replicas)


def update_rps_per_replica_bounds(
    rps_per_replica,
    err,
    max_rps_per_replica_without_err,
    min_rps_per_replica_with_err,
    margin_ratio,
):
    if rps_per_replica <= 0.0:
        return max_rps_per_replica_without_err, min_rps_per_replica_with_err

    if err > 0.0:
        if (
            min_rps_per_replica_with_err <= 0.0
            or rps_per_replica < min_rps_per_replica_with_err
        ):
            min_rps_per_replica_with_err = rps_per_replica
            if max_rps_per_replica_without_err >= min_rps_per_replica_with_err:
                max_rps_per_replica_without_err = max(
                    0.0,
                    min_rps_per_replica_with_err * (1.0 - margin_ratio),
                )
    elif (
        max_rps_per_replica_without_err <= 0.0
        or rps_per_replica > max_rps_per_replica_without_err
    ):
        max_rps_per_replica_without_err = rps_per_replica
        if (
            min_rps_per_replica_with_err > 0.0
            and max_rps_per_replica_without_err >= min_rps_per_replica_with_err
        ):
            min_rps_per_replica_with_err = max_rps_per_replica_without_err * (
                1.0 + margin_ratio
            )

    return max_rps_per_replica_without_err, min_rps_per_replica_with_err


def pid_delta(err, last_err, sum_err, kp, ki, kd):
    output = kp * err + ki * sum_err + kd * (err - last_err)
    if output > 0:
        return math.ceil(output)
    if output < 0:
        return math.floor(output)
    return 0


def min_replicas_below_rps_limit(total_rps, rps_per_replica_limit):
    if total_rps <= 0.0 or rps_per_replica_limit <= 0.0:
        return 0
    return math.floor(total_rps / rps_per_replica_limit) + 1


def evaluate(
    spec,
    redis_host,
    kp,
    ki,
    kd,
    min_replicas,
    max_replicas,
    downscale_stabilization,
    margin_ratio,
):
    try:
        labels = spec["resource"]["metadata"]["labels"]
    except KeyError:
        logging.error("Invalid spec format: missing metadata.labels")
        sys.exit(1)

    group = get_label_value(labels, "group")
    app = get_label_value(labels, "app")
    if len(spec.get("metrics", [])) != 1:
        logging.error("Expected exactly 1 metric in spec")
        sys.exit(1)
    try:
        metric_value = json.loads(spec["metrics"][0]["value"])
    except (KeyError, json.JSONDecodeError) as error:
        logging.error("Invalid metric format: %s", error)
        sys.exit(1)

    total_rps = max(0.0, float(metric_value.get("rps", 0.0)))
    err = float(metric_value.get("err", 0.0))
    avg_ready_replicas = float(metric_value.get("avg_ready_replicas", 1.0))
    if avg_ready_replicas <= 0.0:
        avg_ready_replicas = 1.0

    try:
        state = redis.Redis(
            host=redis_host,
            port=6379,
            db=0,
            decode_responses=True,
        )
        state.ping()
    except redis.RedisError as error:
        logging.error("Failed to connect to Redis: %s", error)
        sys.exit(1)

    try:
        target_replicas = int(
            state.get(redis_key(group, app, "target_replicas")) or min_replicas
        )
        last_err = float(state.get(redis_key(group, app, "last_err")) or 0.0)
        sum_err = float(state.get(redis_key(group, app, "sum_err")) or 0.0)
        min_rps_per_replica_with_err = float(
            state.get(redis_key(group, app, "min_rpspr_with_err")) or 0.0
        )
        max_rps_per_replica_without_err = float(
            state.get(redis_key(group, app, "max_rpspr_without_err")) or 0.0
        )
        last_scale_timestamp = float(
            state.get(redis_key(group, app, "last_scale_timestamp")) or 0.0
        )
    except redis.RedisError as error:
        logging.error("Failed to read autoscaler state from Redis: %s", error)
        sys.exit(1)

    target_replicas = clamp_replicas(
        target_replicas,
        min_replicas,
        max_replicas,
    )
    current_timestamp = time.time()
    rps_per_replica = total_rps / avg_ready_replicas
    (
        max_rps_per_replica_without_err,
        min_rps_per_replica_with_err,
    ) = update_rps_per_replica_bounds(
        rps_per_replica,
        err,
        max_rps_per_replica_without_err,
        min_rps_per_replica_with_err,
        margin_ratio,
    )

    if (
        min_replicas < target_replicas < max_replicas
        or target_replicas == max_replicas
        and err < 0.0
        or target_replicas == min_replicas
        and err > 0.0
    ):
        sum_err += err

    replica_delta = pid_delta(err, last_err, sum_err, kp, ki, kd)
    if replica_delta > 0:
        pid_target_replicas = target_replicas + replica_delta
        if max_rps_per_replica_without_err > 0.0:
            safe_target_replicas = math.ceil(
                total_rps / max_rps_per_replica_without_err
            )
            pid_target_replicas = min(
                pid_target_replicas,
                max(
                    target_replicas + (1 if err > 0.0 else 0),
                    safe_target_replicas,
                ),
            )
        next_target_replicas = clamp_replicas(
            pid_target_replicas,
            min_replicas,
            max_replicas,
        )
        if next_target_replicas != target_replicas:
            target_replicas = next_target_replicas
            last_scale_timestamp = current_timestamp
    elif (
        replica_delta < 0
        and current_timestamp - last_scale_timestamp > downscale_stabilization
    ):
        pid_target_replicas = target_replicas + replica_delta
        min_safe_replicas = min_replicas_below_rps_limit(
            total_rps,
            min_rps_per_replica_with_err,
        )
        next_target_replicas = clamp_replicas(
            max(pid_target_replicas, min_safe_replicas),
            min_replicas,
            max_replicas,
        )
        if next_target_replicas != target_replicas:
            target_replicas = next_target_replicas
            last_scale_timestamp = current_timestamp

    try:
        state.set(redis_key(group, app, "target_replicas"), target_replicas)
        state.set(redis_key(group, app, "last_err"), err)
        state.set(redis_key(group, app, "sum_err"), sum_err)
        state.set(
            redis_key(group, app, "min_rpspr_with_err"),
            min_rps_per_replica_with_err,
        )
        state.set(
            redis_key(group, app, "max_rpspr_without_err"),
            max_rps_per_replica_without_err,
        )
        state.set(
            redis_key(group, app, "last_scale_timestamp"),
            last_scale_timestamp,
        )
        state.set(redis_key(group, app, "last_delta_replicas"), replica_delta)
    except redis.RedisError as error:
        logging.error("Failed to write autoscaler state to Redis: %s", error)
        sys.exit(1)

    print(json.dumps({"targetReplicas": target_replicas}))


def run(config):
    min_replicas = number(config, "minReplicas", int)
    max_replicas = number(config, "maxReplicas", int)
    margin_ratio = float(optional(config, "marginRatio", 0.1))
    if min_replicas < 1 or max_replicas < min_replicas:
        raise ValueError("replicas must satisfy 1 <= minReplicas <= maxReplicas")
    if not 0.0 < margin_ratio < 1.0:
        raise ValueError("marginRatio must be greater than zero and less than one")
    try:
        spec = json.loads(sys.stdin.read())
    except json.JSONDecodeError as error:
        raise ValueError("failed to parse autoscaler request as JSON") from error
    evaluate(
        spec,
        required(config, "redisHost"),
        number(config, "kp"),
        number(config, "ki"),
        number(config, "kd"),
        min_replicas,
        max_replicas,
        number(config, "downscaleStabilizationSeconds"),
        margin_ratio,
    )


def legacy_main():
    parser = argparse.ArgumentParser(
        description="Compute replicas number based on PID control."
    )
    parser.add_argument("--redis_host", required=True)
    parser.add_argument("--kp", required=True, type=float)
    parser.add_argument("--ki", required=True, type=float)
    parser.add_argument("--kd", required=True, type=float)
    parser.add_argument("--min_replicas", required=True, type=int)
    parser.add_argument("--max_replicas", required=True, type=int)
    parser.add_argument("--downscale_stabilization", required=True, type=float)
    parser.add_argument("--margin_ratio", type=float, default=0.1)
    args = parser.parse_args()
    if not 0.0 < args.margin_ratio < 1.0:
        parser.error("--margin_ratio must be greater than zero and less than one")
    try:
        spec = json.loads(sys.stdin.read())
    except json.JSONDecodeError:
        logging.error("Failed to parse JSON from stdin")
        sys.exit(1)
    evaluate(
        spec,
        args.redis_host,
        args.kp,
        args.ki,
        args.kd,
        args.min_replicas,
        args.max_replicas,
        args.downscale_stabilization,
        args.margin_ratio,
    )
