import argparse
import importlib
import json
import logging
import re
import sys


PLUGIN_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def load_plugin(name, phase):
    if not PLUGIN_NAME.fullmatch(name):
        raise ValueError(f"invalid autoscaler plugin name: {name!r}")
    try:
        module = importlib.import_module(f"plugins.{name}.{phase}")
    except ModuleNotFoundError as error:
        expected = f"plugins.{name}"
        missing_module = error.name or ""
        if missing_module == expected or missing_module.startswith(expected + "."):
            raise ValueError(f"autoscaler plugin {name!r} does not implement {phase}") from error
        raise
    run = getattr(module, "run", None)
    if not callable(run):
        raise ValueError(f"autoscaler plugin {name!r} has no callable {phase}.run")
    return run


def load_config(path):
    try:
        with open(path, encoding="utf-8") as config_file:
            document = json.load(config_file)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"failed to read plugin configuration: {error}") from error
    if not isinstance(document, dict):
        raise ValueError("plugin configuration must be a JSON object")
    plugin = document.get("plugin")
    config = document.get("config")
    if not isinstance(plugin, str) or not plugin:
        raise ValueError("plugin configuration must define plugin")
    if not isinstance(config, dict):
        raise ValueError("plugin configuration must define a config object")
    return plugin, config


def main():
    parser = argparse.ArgumentParser(description="Run an autoscaler plugin phase.")
    parser.add_argument("phase", choices=("metric", "evaluate"))
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    try:
        plugin, config = load_config(args.config)
        load_plugin(plugin, args.phase)(config)
    except ValueError as error:
        logging.error("Invalid plugin configuration: %s", error)
        sys.exit(2)


if __name__ == "__main__":
    main()
