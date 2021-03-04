from pathlib import Path
import sys
from time import time
from typing import TextIO, Union

import click
from tqdm import tqdm

from aiida_optimade.cli.cmd_aiida_optimade import cli
from aiida_optimade.common.logger import disable_logging, LOGGER


ITEM_SEPARATOR = ","
KEY_SEPARATOR = ":"


@cli.command()
@click.argument(
    "filename",
    type=click.Path(dir_okay=False, resolve_path=True, allow_dash=True),
    required=True,
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    show_default=True,
    help=(
        "Overwrite existing filename if necessary (not relevant if --piping is defined"
        ")."
    ),
)
@click.option(
    "--batch-size",
    type=click.INT,
    default=100,
    show_default=True,
    help="Batch size for QueryBuilder.iterall() and when to write to file.",
)
@click.option(
    "--as-is",
    is_flag=True,
    default=False,
    show_default=True,
    help=(
        "Store calculated OPTIMADE fields as is, e.g., do not check that species.mass "
        "is up-to-date with OPTIMADE API v1.0.1."
    ),
)
@click.pass_obj
def mongofy(
    obj: dict,
    filename: str,
    force: bool,
    batch_size: int,
    as_is: bool,
):
    """Create MongoDB-importable JSON output from an already initialized AiiDA DB.

    FILENAME is the name (including suffix) for the JSON file to be created, optionally
    including path. (Use '-' for stdout/piping).
    """
    from aiida import load_profile
    from aiida.cmdline.utils import echo

    try:
        profile: str = obj.get("profile").name
    except AttributeError:
        profile = None
    profile = load_profile(profile).name

    if isinstance(filename, str):
        if filename == "-":
            filename: TextIO = sys.stdout
            piping = True
        else:
            filename: Path = Path(filename)
            piping = False

    if not piping:
        if filename.exists():
            if force:
                if not piping:
                    echo.echo_warning(f"Removing existing file at {filename}")
                filename.unlink()
                if filename.exists():
                    echo.echo_critical(
                        f"Unlinked (removed) file at {filename}, but it still "
                        "registers as existing."
                    )
            else:
                echo.echo_critical(
                    f"{filename} already exists! (Use --force if you wish to overwrite "
                    "the file.)"
                )

        # Overview
        echo.echo_info(f"File: {filename}")
        echo.echo_info(f"Batch size: {batch_size}")
        echo.echo_info(f"Store fields as is: {as_is}")

    try:
        start = time()
        create_mongodb(
            output=filename, batch_size=batch_size, as_is=as_is, silent=piping
        )
    except click.Abort:
        echo.echo_warning("Aborted!", err=True)
        return
    except Exception as exc:  # pylint: disable=broad-except
        import traceback

        LOGGER.error(
            "Full exception from 'aiida-optimade mongofy' CLI:\n%s",
            traceback.format_exc(),
        )
        echo.echo_critical(
            f"An error occurred trying outputting to {'stdout' if piping else filename}"
            f" for {profile!r} (see log for more details):\n{exc!r}"
        )

    end = time() - start
    days = int(end / (60 * 60 * 24))
    hours = int((end / (60 * 60 * 24) - days) * 24)
    minutes = int((((end / (60 * 60 * 24) - days) * 24) - hours) * 60)
    seconds = (((((end / (60 * 60 * 24) - days) * 24) - hours) * 60) - minutes) * 60

    LOGGER.debug(
        "Time taken for 'aiida-optimade mongofy': %f s (%s days %s hours %s min %.1f "
        "seconds)",
        end,
        days,
        hours,
        minutes,
        seconds,
    )

    if not piping:
        file_size = filename.stat().st_size
        echo.echo_success(f"Generated {filename.name} !")
        echo.echo_success(
            f"Time taken: {end:.1f} s{' (' if minutes else ''}"
            f"{str(days) + ' days ' if days else ''}"
            f"{str(hours) + ' hours ' if hours else ''}"
            f"{'%s min %.1f seconds)' % (minutes, seconds) if minutes else ''}"
        )
        echo.echo_success(
            f"Size: {file_size} bytes ("
            f"{'%.1f GB' % (file_size / 10**9) if file_size >= 10**9 else '%.1f MB' % (file_size / 10**6)})"  # pylint: disable=line-too-long
        )


def write_data(output: Union[Path, TextIO], data: str, mode: str = None) -> None:
    """Write data to output using mode

    Parameters:
        output: Filename or text stream to write data to.
        data: The data to write (shouldn't be binary).
        mode: Equivalent to the built-in `open()` function's `mode` parameter
            (default: "w").

    """
    mode = mode if mode and isinstance(mode, str) else "w"
    if isinstance(output, Path):
        with open(output, mode=mode) as handle:
            handle.write(data)
    elif output in {sys.stdout, sys.stderr}:
        output.write(data)
    else:
        raise TypeError("output must of either type str or TextIO (stdout, stderr).")


def create_mongodb(  # pylint: disable=too-many-branches,too-many-locals
    output: Union[Path, TextIO],
    batch_size: int,
    as_is: bool = False,
    silent: bool = False,
) -> None:
    """Create JSON output

    Parameters:
        output: Where to write the created MongoDB JSON to.
            If STDOUT, then avoid any informational print statements writing to stdout.
            This can be used for piping in a (bash) shell, e.g., for zipping.
        batch_size: Batch size for QueryBuilder.iterall() and when to write to output.
        as_is: Whether or not to use the calculated OPTIMADE fields as is or not.
            If False, then:
            - Ensure `species.mass` is up-to-date with v1.0.1.
        silent: Whether or not to use a progress bar and print statements.

    """
    import bson.json_util
    from aiida.orm.querybuilder import QueryBuilder
    from aiida_optimade.translators.utils import hex_to_floats

    float_fields = {
        "elements_ratios",
        "lattice_vectors",
        "cartesian_site_positions",
    }

    write_data(output, "[", mode="x")

    try:  # pylint: disable=too-many-nested-blocks
        with disable_logging():
            from aiida_optimade.routers.structures import STRUCTURES

        data = []
        builder = QueryBuilder().append(entity_type=tuple(STRUCTURES.entities))

        structures = (
            builder.iterall(batch_size=batch_size)
            if silent
            else tqdm(
                builder.iterall(batch_size=batch_size),
                desc="Writing structures",
                leave=False,
                total=builder.count(),
            )
        )

        for (structure,) in structures:
            new_doc: dict = structure.get_extra("optimade")
            new_doc.update(
                {
                    "id": str(structure.pk),
                    "immutable_id": structure.uuid,
                    "last_modified": structure.mtime,
                    "ctime": structure.ctime,
                }
            )
            del structure

            # Always convert hex values to floats
            for field in float_fields:
                if new_doc.get(field) is not None:
                    new_doc[field] = hex_to_floats(new_doc[field])

            if not as_is:
                # Check species.mass
                if new_doc.get("species", False):
                    for species in new_doc["species"] or []:
                        if (
                            isinstance(species.get("mass"), list)
                            or species.get("mass") is None
                        ):
                            continue
                        if len(
                            species.get("chemical_symbols", [])
                        ) == 2 and "vacancy" in species.get("chemical_symbols", []):
                            species["mass"] = [species["mass"]]
                            species["mass"].insert(
                                species["chemical_symbols"].index("vacancy"), 0.0
                            )
                        elif len(species.get("chemical_symbols", [])) == 1:
                            species["mass"] = [species["mass"]]
                        else:
                            species["mass"] = None
                # NOTE: Further "corrections" can be added here ...

            data.append(new_doc)
            del new_doc

            if len(data) == batch_size:
                # "Flush"
                to_write = bson.json_util.dumps(
                    data,
                    separators=(ITEM_SEPARATOR, KEY_SEPARATOR),
                )[1:-1]
                write_data(output, to_write + ITEM_SEPARATOR, mode="a")
                del to_write
                del data
                data = []

        # Make sure to get all entries in
        if data:
            to_write = bson.json_util.dumps(
                data,
                separators=(ITEM_SEPARATOR, KEY_SEPARATOR),
            )[1:-1]
            write_data(output, to_write, mode="a")
            del to_write
            del data
    finally:
        write_data(output, "]", mode="a")
