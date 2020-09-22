import pprint

import typer
from halo import Halo
from tabulate import tabulate

import preserve


app = typer.Typer()

# TODO better implementation of multi
# TODO support with statements in preserve.open
# TODO documentation
# TODO tests


@app.callback()
def callback():
    """🥫 Preserve - A simple Key/Value database with multiple backends."""


@app.command(help="Export a database to a different database.")
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

    with Halo(
        text=f"Opening output preserve: {output}.", spinner="dots"
    ) as sp:
        out_preserve_db = preserve.from_uri(output)
        sp.succeed()

    with typer.progressbar(
        list(in_preserve_db), label="Exporting data"
    ) as progress:
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


@app.command(help="Get header of a given database table.")
def header(
    uri: str = typer.Argument(
        ..., help="The URI specifying how to access the preserve database."
    ),
    rows: int = typer.Option(
        10, "--nb", "-n", help="The number of rows to display."
    ),
):
    pp = pprint.PrettyPrinter(width=41, compact=True)

    preserve_db = None
    with Halo(text=f"Opening preserve: {uri}.", spinner="dots") as sp:
        preserve_db = preserve.from_uri(uri)
        sp.succeed()

    if preserve_db:

        with Halo(text="Fetching head.", spinner="dots") as sp:
            values = [
                [idx, pp.pformat(preserve_db[idx])]
                for idx in list(preserve_db)[: min(len(preserve_db), rows)]
            ]
            sp.succeed()

            typer.echo(
                tabulate(
                    values,
                    headers=["key", "value"],
                    showindex="never",
                    tablefmt="psql",
                )
            )

            preserve_db.close()


if __name__ == "__main__":
    app()
