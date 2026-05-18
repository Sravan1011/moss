"""moss completions — generate shell completion scripts for Bash and Zsh."""

from __future__ import annotations

import click
import typer
from rich.console import Console

console = Console()
completions_app = typer.Typer(
    name="completions",
    help="Generate shell completion scripts for Bash and Zsh.",
    no_args_is_help=True,
)


def _get_completion_script(shell: str) -> str:
    """Generate the completion script for the given shell.

    Uses Click's shell_completion classes to generate the script, then
    patches the env-var instruction format to match Typer's runtime
    parser which uses the legacy ``instruction_shell`` order
    (e.g. ``complete_bash`` instead of Click 8's ``bash_complete``).
    """
    from ..main import app

    # Click 8.x shell completion classes
    shell_cls = click.shell_completion.get_completion_class(shell)
    if shell_cls is None:
        raise typer.BadParameter(f"Unsupported shell: {shell}")

    # Build the completion instance from the root Click command.
    cli = typer.main.get_command(app)
    comp = shell_cls(
        cli=cli,
        ctx_args={},
        prog_name="moss",
        complete_var="_MOSS_COMPLETE",
    )
    script = comp.source()

    # Typer's completion.shell_complete() parses the env var value as
    # ``instruction_shell`` (e.g. "complete_bash"), but Click 8.x generates
    # scripts using ``shell_instruction`` (e.g. "bash_complete").
    # Patch the generated script to use Typer's expected format.
    script = script.replace(f"{shell}_complete", f"complete_{shell}")
    script = script.replace(f"{shell}_source", f"source_{shell}")

    return script


@completions_app.command(name="bash")
def bash_completion() -> None:
    """Output the Bash completion script.

    Usage:
        moss completions bash >> ~/.bashrc
        eval "$(moss completions bash)"
    """
    script = _get_completion_script("bash")
    click.echo(script)


@completions_app.command(name="zsh")
def zsh_completion() -> None:
    """Output the Zsh completion script.

    Usage:
        moss completions zsh >> ~/.zshrc
        eval "$(moss completions zsh)"
    """
    script = _get_completion_script("zsh")
    click.echo(script)


@completions_app.command(name="install")
def install_completion(
    shell: str = typer.Argument(
        None,
        help="Shell to install completions for (bash or zsh). Auto-detects if omitted.",
    ),
) -> None:
    """Install completion script for your current shell.

    Auto-detects the shell if not specified. Writes the completion script
    to the appropriate shell config file.
    """
    if shell is None:
        try:
            import shellingham

            shell, _ = shellingham.detect_shell()
        except Exception:
            console.print(
                "[red]Could not detect shell. Please specify: "
                "moss completions install bash[/red]"
            )
            raise typer.Exit(1)

    shell = shell.lower()
    if shell not in ("bash", "zsh"):
        console.print(f"[red]Unsupported shell: {shell}. Use 'bash' or 'zsh'.[/red]")
        raise typer.Exit(1)

    script = _get_completion_script(shell)

    import os
    from pathlib import Path

    if shell == "bash":
        rc_path = Path.home() / ".bashrc"
    else:
        rc_path = Path.home() / ".zshrc"

    marker = "# >>> moss completions >>>"
    end_marker = "# <<< moss completions <<<"

    existing = ""
    if rc_path.exists():
        existing = rc_path.read_text()

    # Remove old completions block if present
    if marker in existing:
        import re

        pattern = re.escape(marker) + r".*?" + re.escape(end_marker) + r"\n?"
        existing = re.sub(pattern, "", existing, flags=re.DOTALL)

    block = f"\n{marker}\n{script}\n{end_marker}\n"
    rc_path.write_text(existing + block)

    console.print(
        f"[green]Completions installed to {rc_path}[/green]\n"
        f"[dim]Restart your shell or run: source {rc_path}[/dim]"
    )


@completions_app.command(name="show")
def show_completion(
    shell: str = typer.Argument(
        None,
        help="Shell to show completions for (bash or zsh). Auto-detects if omitted.",
    ),
) -> None:
    """Show the completion script for your shell (auto-detects if not specified)."""
    if shell is None:
        try:
            import shellingham

            shell, _ = shellingham.detect_shell()
        except Exception:
            console.print(
                "[red]Could not detect shell. Please specify: "
                "moss completions show bash[/red]"
            )
            raise typer.Exit(1)

    shell = shell.lower()
    if shell not in ("bash", "zsh"):
        console.print(f"[red]Unsupported shell: {shell}. Use 'bash' or 'zsh'.[/red]")
        raise typer.Exit(1)

    script = _get_completion_script(shell)
    click.echo(script)
