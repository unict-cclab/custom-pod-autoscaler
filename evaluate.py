import json
import sys
import argparse
import logging
import math
import time
import redis

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_label_value(labels, key):
    if key not in labels:
        logging.error(f"No '{key}' label on resource being managed")
        sys.exit(1)
    return labels[key]


def redis_key(group, app, suffix):
    return f"{group}_{app}_{suffix}"


def finite_number(value, default):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(parsed):
        return default
    return parsed


def clamp_replicas(value, min_replicas, max_replicas):
    return max(min(value, max_replicas), min_replicas)


def update_rps_per_replica_bounds(rps_per_replica, err, safe_rpspr, unsafe_rpspr):
    if rps_per_replica <= 0.0:
        return safe_rpspr, unsafe_rpspr

    if err > 0.0:
        if unsafe_rpspr <= 0.0 or rps_per_replica < unsafe_rpspr:
            unsafe_rpspr = rps_per_replica
            if safe_rpspr > 0.0 and safe_rpspr > unsafe_rpspr:
                safe_rpspr = 0.0
    else:
        if safe_rpspr <= 0.0 or rps_per_replica > safe_rpspr:
            safe_rpspr = rps_per_replica
            if unsafe_rpspr > 0.0 and safe_rpspr > unsafe_rpspr:
                unsafe_rpspr = 0.0

    return safe_rpspr, unsafe_rpspr


def pid_delta(err, last_err, sum_err, kp, ki, kd):
    output = kp * err + ki * sum_err + kd * (err - last_err)
    if output > 0:
        return max(1, math.ceil(output))
    if output < 0:
        return min(-1, math.floor(output))
    return 0


def replicas_for_safe_rps(rps, safe_rpspr):
    if rps <= 0.0 or safe_rpspr <= 0.0:
        return 0
    return math.ceil(rps / safe_rpspr)


def replicas_above_unsafe_rps(rps, unsafe_rpspr):
    if rps <= 0.0 or unsafe_rpspr <= 0.0:
        return 0
    return math.floor(rps / unsafe_rpspr) + 1


def evaluate(
    spec,
    redis_host,
    kp,
    ki,
    kd,
    min_replicas,
    max_replicas,
    downscale_stabilization,
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
    except (KeyError, json.JSONDecodeError) as e:
        logging.error(f"Invalid metric format: {e}")
        sys.exit(1)

    rps = finite_number(metric_value.get("rps"), 0.0)
    if rps < 0.0:
        rps = 0.0

    err = finite_number(metric_value.get("error"), 0.0)

    avg_replicas = finite_number(metric_value.get("avg_replicas"), 1.0)
    if avg_replicas <= 0.0:
        avg_replicas = 1.0

    try:
        r = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        r.ping()
    except redis.RedisError as e:
        logging.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)

    try:
        target_replicas = int(finite_number(r.get(redis_key(group, app, "target_replicas")), min_replicas))
        last_err = finite_number(r.get(redis_key(group, app, "last_err")), 0.0)
        sum_err = finite_number(r.get(redis_key(group, app, "sum_err")), 0.0)
        unsafe_rpspr = finite_number(r.get(redis_key(group, app, "min_rpspr_with_err")), 0.0)
        safe_rpspr = finite_number(r.get(redis_key(group, app, "max_rpspr_without_err")), 0.0)
        last_scale_timestamp = finite_number(r.get(redis_key(group, app, "last_scale_timestamp")), 0.0)
    except redis.RedisError as e:
        logging.error(f"Failed to read autoscaler state from Redis: {e}")
        sys.exit(1)

    target_replicas = clamp_replicas(target_replicas, min_replicas, max_replicas)
    current_timestamp = time.time()
    rps_per_replica = rps / avg_replicas

    safe_rpspr, unsafe_rpspr = update_rps_per_replica_bounds(
        rps_per_replica,
        err,
        safe_rpspr,
        unsafe_rpspr,
    )

    if min_replicas < target_replicas < max_replicas:
        sum_err += err

    delta_replicas = pid_delta(err, last_err, sum_err, kp, ki, kd)

    if delta_replicas > 0:
        pid_target = target_replicas + delta_replicas
        safe_target = replicas_for_safe_rps(rps, safe_rpspr)
        target_replicas = clamp_replicas(max(pid_target, safe_target), min_replicas, max_replicas)
        last_scale_timestamp = current_timestamp
    elif delta_replicas < 0 and current_timestamp - last_scale_timestamp > downscale_stabilization:
        pid_target = target_replicas + delta_replicas
        unsafe_floor = replicas_above_unsafe_rps(rps, unsafe_rpspr)
        target_replicas = clamp_replicas(max(pid_target, unsafe_floor), min_replicas, max_replicas)
        last_scale_timestamp = current_timestamp

    try:
        r.set(redis_key(group, app, "target_replicas"), target_replicas)
        r.set(redis_key(group, app, "last_err"), err)
        r.set(redis_key(group, app, "sum_err"), sum_err)
        r.set(redis_key(group, app, "min_rpspr_with_err"), unsafe_rpspr)
        r.set(redis_key(group, app, "max_rpspr_without_err"), safe_rpspr)
        r.set(redis_key(group, app, "last_scale_timestamp"), last_scale_timestamp)
        r.set(redis_key(group, app, "last_delta_replicas"), delta_replicas)
    except redis.RedisError as e:
        logging.error(f"Failed to write autoscaler state to Redis: {e}")
        sys.exit(1)

    evaluation = {"targetReplicas": target_replicas}

    print(json.dumps(evaluation))


def main():
    parser = argparse.ArgumentParser(description="Compute replicas number based on PID control.")
    parser.add_argument("--redis_host", required=True, help="Redis host.")
    parser.add_argument("--kp", required=True, type=float, help="Proportional controller gain.")
    parser.add_argument("--ki", required=True, type=float, help="Integral controller gain.")
    parser.add_argument("--kd", required=True, type=float, help="Derivative controller gain.")
    parser.add_argument("--min_replicas", required=True, type=int, help="Minimum replicas.")
    parser.add_argument("--max_replicas", required=True, type=int, help="Maximum replicas.")
    parser.add_argument("--downscale_stabilization", required=True, type=float, help="Downscale stabilization.")
    args = parser.parse_args()

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
    )

if __name__ == "__main__":
    main()
