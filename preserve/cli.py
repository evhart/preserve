import click
from halo import Halo
from tabulate import tabulate
from time import sleep

import preserve
import pprint


# @click.group(help="Preserve")
# def cli():
#     pass


# @cli.command(help="Get header of a given database table.")
# @click.option(
#     "-p",
#     "--path",
#     required=True,
#     default="preserve.dbm",
#     type=str,
#     help="Database path.",
# )
# @click.option(
#     "-n", "--nb", required=True, default=10, type=int, help="Number of items to fetch."
# )
# def header2(path: str = "preserve.dbm", nb: int = 10):
#     pp = pprint.PrettyPrinter(width=41, compact=True)

#     with Halo(text=f"Opening preserve: '{path}'.'", spinner="dots") as sp:
#         preserve_db = preserve.open("shelf", filename=path)
#         sp.succeed()

#     with Halo(
#         text=f"Fetching {nb} item{'s' if nb > 1 else ''} from head.'", spinner="dots"
#     ) as sp:
#         values = [
#             [idx, pp.pformat(preserve_db[idx])]
#             for idx in list(preserve_db)[: min(len(preserve_db), nb)]
#         ]
#         sp.succeed()

#     click.echo(f"🔎 Showing {min(len(preserve_db), nb)} of {len(preserve_db)} items.")
#     click.echo(
#         tabulate(values, headers=["key", "value"], showindex="never", tablefmt="psql")
#     )
#     preserve_db.close()


# @cli.command(help="Get header of a given database table.")
# @click.option(
#     "-p",
#     "--path",
#     required=True,
#     default="preserve.dbm",
#     type=str,
#     help="Database path.",
# )
# @click.option(
#     "-t", "--table", required=True, default="default", type=str, help="Table name."
# )
# def header(path: str = "preserve.dbm", table: str = "default"):
#     pp = pprint.PrettyPrinter(width=41, compact=True)

#     with Halo(text=f"Opening preserve: '{path}'.'", spinner="dots") as sp:
#         preserve_db = preserve.open("multi-shelf", filename=path)
#         sp.succeed()

#     with Halo(text=f"Fetching head for table: '{table}'.'", spinner="dots") as sp:
#         values = [
#             [idx, pp.pformat(preserve_db[table][idx])]
#             for idx in list(preserve_db[table])[: min(len(preserve_db[table]), 10)]
#         ]
#         sp.succeed()

#     click.echo(
#         tabulate(values, headers=["key", "value"], showindex="never", tablefmt="psql")
#     )
#     preserve_db.close()


# if __name__ == "__main__":
#     cli()

from typing import Optional
import typer

app = typer.Typer()

# TODO better implementation of multi
# TODO support with statements in preserve.open
# TODO documentation
# TODO tests


@app.callback()
def callback():
    """
    🥫 Preserve - A simple Key/Value database with multiple backends.
    """


@app.command(
    help="Export a database to a different output (e.g. migrating from shelf to mongo)."
)
def export(
    input: str = typer.Option(
        ...,
        "--input",
        "-i",
        help="The URI specifying how to access the input preserve database.",
    ),
    output: str = typer.Option(
        ...,
        "--output",
        "-o",
        help="The URI specifying how to access the input preserve database.",
    ),
):
    assert input != output

    with Halo(text=f"Opening input preserve: {input}.", spinner="dots") as sp:
        in_preserve_db = preserve.from_uri(input)
        sp.succeed()

    with Halo(text=f"Opening output preserve: {output}.", spinner="dots") as sp:
        out_preserve_db = preserve.from_uri(output)
        sp.succeed()

    with typer.progressbar(list(in_preserve_db), label="Exporting data") as progress:
        for idx in progress:
            out_preserve_db[idx] = in_preserve_db[idx]

    in_preserve_db.close()
    out_preserve_db.close()


@app.command(help="List available connectors.")
def connectors(
    # connector: Optional[str] = typer.Option(
    #    None, "--connector", "-c", help="The connector to inspect."
    # )
):

    pp = pprint.PrettyPrinter(width=41, compact=True)

    values = []
    for c in preserve.connectors():
        sc = c.schema()
        props_values = []
        for n, p in sc["properties"].items():
            props = [n, f"{p['type'].upper()}"]
            if "default" in p:
                props.append(f"[default: {p['default']}]")

            props_values.append(props)

        values.append(
            [
                c.scheme(),
                sc["description"],
                tabulate(props_values, showindex="never", tablefmt="plain"),
            ]
        )

    typer.echo(
        tabulate(
            values,
            headers=["scheme", "description", "parameters"],
            showindex="never",
            tablefmt="psql",
        )
    )


# @app.command(help="Get header of a given database table.")
# def header(
#     connector: str = typer.Option(
#         "multi-shelf", "--connector", "-c", help="The connector to use."
#     ),
#     path: str = typer.Option("preserve.dbm", "--path", "-p", help="Database path."),
#     table: Optional[str] = typer.Option(
#         "default", "--table", "-t", help="Name of the table to display."
#     ),
# ):
#     pp = pprint.PrettyPrinter(width=41, compact=True)

#     with Halo(text=f"Opening preserve: '{path}'.'", spinner="dots") as sp:
#         if "shelf" in connector:
#             preserve_db = preserve.open(connector, filename=path)
#         else:
#             preserve_db = preserve.open(connector)
#         sp.succeed()

#     if connector.startswith("multi-"):
#         with Halo(text=f"Fetching head for table: '{table}'.'", spinner="dots") as sp:
#             values = [
#                 [idx, pp.pformat(preserve_db[table][idx])]
#                 for idx in list(preserve_db[table])[: min(len(preserve_db[table]), 10)]
#             ]
#             sp.succeed()
#     else:
#         with Halo(text=f"Fetching head.'", spinner="dots") as sp:
#             values = [
#                 [idx, pp.pformat(preserve_db[idx])]
#                 for idx in list(preserve_db)[: min(len(preserve_db), 10)]
#             ]
#             sp.succeed()

#     typer.echo(
#         tabulate(values, headers=["key", "value"], showindex="never", tablefmt="psql")
#     )
#     preserve_db.close()


@app.command(help="Get header of a given database table.")
def header(
    uri: str = typer.Argument(
        ..., help="The URI specifying how to access the preserve database."
    ),
    rows: int = typer.Option(10, "--nb", "-n", help="The number of rows to display."),
):
    pp = pprint.PrettyPrinter(width=41, compact=True)

    preserve_db = None
    with Halo(text=f"Opening preserve: {uri}.", spinner="dots") as sp:
        preserve_db = preserve.from_uri(uri)
        sp.succeed()

    if preserve_db:

        with Halo(text=f"Fetching head.", spinner="dots") as sp:
            values = [
                [idx, pp.pformat(preserve_db[idx])]
                for idx in list(preserve_db)[: min(len(preserve_db), rows)]
            ]
            sp.succeed()

            typer.echo(
                tabulate(
                    values, headers=["key", "value"], showindex="never", tablefmt="psql"
                )
            )

            preserve_db.close()


if __name__ == "__main__":
    app()
