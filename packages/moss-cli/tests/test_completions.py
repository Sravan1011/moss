"""Tests for the moss completions command and dynamic index name completion."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import typer
from typer.testing import CliRunner

from moss_cli.main import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# moss completions bash / zsh — script output
# ---------------------------------------------------------------------------


class TestBashCompletion:
    """Tests for `moss completions bash`."""

    def test_outputs_script(self) -> None:
        result = runner.invoke(app, ["completions", "bash"])
        assert result.exit_code == 0
        # Bash completion scripts define a function or use complete builtin
        assert "_MOSS" in result.output or "complete" in result.output

    def test_script_is_not_empty(self) -> None:
        result = runner.invoke(app, ["completions", "bash"])
        assert result.exit_code == 0
        assert len(result.output.strip()) > 50  # Non-trivial output


class TestZshCompletion:
    """Tests for `moss completions zsh`."""

    def test_outputs_script(self) -> None:
        result = runner.invoke(app, ["completions", "zsh"])
        assert result.exit_code == 0
        # Zsh completion scripts typically contain compdef or compadd
        assert "_MOSS" in result.output or "compdef" in result.output or "compadd" in result.output

    def test_script_is_not_empty(self) -> None:
        result = runner.invoke(app, ["completions", "zsh"])
        assert result.exit_code == 0
        assert len(result.output.strip()) > 50


# ---------------------------------------------------------------------------
# moss completions show
# ---------------------------------------------------------------------------


class TestShowCompletion:
    """Tests for `moss completions show <shell>`."""

    def test_show_bash(self) -> None:
        result = runner.invoke(app, ["completions", "show", "bash"])
        assert result.exit_code == 0
        assert len(result.output.strip()) > 50

    def test_show_zsh(self) -> None:
        result = runner.invoke(app, ["completions", "show", "zsh"])
        assert result.exit_code == 0
        assert len(result.output.strip()) > 50

    def test_show_unsupported_shell(self) -> None:
        result = runner.invoke(app, ["completions", "show", "powershell"])
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# moss completions install
# ---------------------------------------------------------------------------


class TestInstallCompletion:
    """Tests for `moss completions install`."""

    def test_install_bash(self, tmp_path: object) -> None:
        import os
        from pathlib import Path

        # Create a temporary bashrc
        fake_home = Path(str(tmp_path))
        bashrc = fake_home / ".bashrc"
        bashrc.write_text("# existing content\n")

        with patch("pathlib.Path.home", return_value=fake_home):
            result = runner.invoke(app, ["completions", "install", "bash"])

        assert result.exit_code == 0
        content = bashrc.read_text()
        assert "# >>> moss completions >>>" in content
        assert "# <<< moss completions <<<" in content
        assert "# existing content" in content

    def test_install_zsh(self, tmp_path: object) -> None:
        from pathlib import Path

        fake_home = Path(str(tmp_path))
        zshrc = fake_home / ".zshrc"
        zshrc.write_text("# existing zsh content\n")

        with patch("pathlib.Path.home", return_value=fake_home):
            result = runner.invoke(app, ["completions", "install", "zsh"])

        assert result.exit_code == 0
        content = zshrc.read_text()
        assert "# >>> moss completions >>>" in content
        assert "# existing zsh content" in content

    def test_install_replaces_old_block(self, tmp_path: object) -> None:
        from pathlib import Path

        fake_home = Path(str(tmp_path))
        bashrc = fake_home / ".bashrc"
        bashrc.write_text(
            "# before\n"
            "# >>> moss completions >>>\n"
            "old script content\n"
            "# <<< moss completions <<<\n"
            "# after\n"
        )

        with patch("pathlib.Path.home", return_value=fake_home):
            result = runner.invoke(app, ["completions", "install", "bash"])

        assert result.exit_code == 0
        content = bashrc.read_text()
        # Old content should be replaced, not duplicated
        assert content.count("# >>> moss completions >>>") == 1
        assert "old script content" not in content
        assert "# before" in content
        assert "# after" in content

    def test_install_unsupported_shell(self) -> None:
        result = runner.invoke(app, ["completions", "install", "fish"])
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# moss completions (no args) — shows help
# ---------------------------------------------------------------------------


class TestCompletionsNoArgs:
    """Test that `moss completions` with no args shows help."""

    def test_no_args_shows_help(self) -> None:
        result = runner.invoke(app, ["completions"])
        # Typer/Click uses exit code 2 for "no_args_is_help"
        assert result.exit_code in (0, 2)
        assert "bash" in result.output.lower()
        assert "zsh" in result.output.lower()


# ---------------------------------------------------------------------------
# Dynamic index name completion callback
# ---------------------------------------------------------------------------


class TestCompleteIndexName:
    """Tests for the complete_index_name() autocompletion callback."""

    def test_returns_matching_names(self) -> None:
        mock_index1 = MagicMock()
        mock_index1.name = "my-products"
        mock_index2 = MagicMock()
        mock_index2.name = "my-docs"
        mock_index3 = MagicMock()
        mock_index3.name = "other-index"

        mock_client = MagicMock()
        mock_client.list_indexes = AsyncMock(
            return_value=[mock_index1, mock_index2, mock_index3]
        )

        with (
            patch.dict(
                "os.environ",
                {"MOSS_PROJECT_ID": "test-pid", "MOSS_PROJECT_KEY": "test-pkey"},
            ),
            patch("moss.MossClient", return_value=mock_client),
        ):
            from moss_cli.commands._completions import complete_index_name

            results = complete_index_name("my")

        assert "my-products" in results
        assert "my-docs" in results
        assert "other-index" not in results

    def test_returns_all_on_empty_prefix(self) -> None:
        mock_index1 = MagicMock()
        mock_index1.name = "index-a"
        mock_index2 = MagicMock()
        mock_index2.name = "index-b"

        mock_client = MagicMock()
        mock_client.list_indexes = AsyncMock(
            return_value=[mock_index1, mock_index2]
        )

        with (
            patch.dict(
                "os.environ",
                {"MOSS_PROJECT_ID": "test-pid", "MOSS_PROJECT_KEY": "test-pkey"},
            ),
            patch("moss.MossClient", return_value=mock_client),
        ):
            from moss_cli.commands._completions import complete_index_name

            results = complete_index_name("")

        assert "index-a" in results
        assert "index-b" in results

    def test_returns_empty_on_no_credentials(self) -> None:
        with (
            patch.dict(
                "os.environ",
                {"MOSS_PROJECT_ID": "", "MOSS_PROJECT_KEY": ""},
                clear=False,
            ),
            patch(
                "moss_cli.config.get_profile_credentials",
                return_value=(None, None),
            ),
        ):
            from moss_cli.commands._completions import complete_index_name

            results = complete_index_name("test")

        assert results == []

    def test_returns_empty_on_api_error(self) -> None:
        mock_client = MagicMock()
        mock_client.list_indexes = AsyncMock(side_effect=Exception("API error"))

        with (
            patch.dict(
                "os.environ",
                {"MOSS_PROJECT_ID": "test-pid", "MOSS_PROJECT_KEY": "test-pkey"},
            ),
            patch("moss.MossClient", return_value=mock_client),
        ):
            from moss_cli.commands._completions import complete_index_name

            results = complete_index_name("test")

        assert results == []
