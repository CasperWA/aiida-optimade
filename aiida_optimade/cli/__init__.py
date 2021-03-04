# pylint: disable=wrong-import-position
import click_completion

# Activate the completion of parameter types provided by the click_completion package
click_completion.init()

# Import to populate sub commands
from aiida_optimade.cli import (  # noqa: E402,F401
    cmd_calc,
    cmd_init,
    cmd_mongofy,
    cmd_run,
)
