def required(config, name):
    value = config.get(name)
    if value is None or isinstance(value, str) and value.strip() == "":
        raise ValueError(f"missing required configuration: {name}")
    return value


def optional(config, name, default):
    value = config.get(name)
    return default if value is None else value


def number(config, name, parser=float):
    value = required(config, name)
    try:
        return parser(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a valid {parser.__name__}") from error


def boolean(config, name, default=False):
    value = optional(config, name, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise ValueError(f"{name} must be true or false")
