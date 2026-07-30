"""Compatibility entrypoint for the Sophos metric plugin."""

import sys

from plugins.sophos import metric as implementation


if __name__ == "__main__":
    implementation.legacy_main()
else:
    sys.modules[__name__] = implementation
