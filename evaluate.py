"""Compatibility entrypoint for the Sophos evaluate plugin."""

import sys

from plugins.sophos import evaluate as implementation


if __name__ == "__main__":
    implementation.legacy_main()
else:
    sys.modules[__name__] = implementation
