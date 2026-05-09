"""Tests for shared runtime path helpers."""

from pathlib import Path

from opx_chain.paths import get_data_dir, get_runs_dir


ROOT = Path(__file__).resolve().parents[1]


def test_get_runs_dir_defaults_to_xdg_data_home(monkeypatch, tmp_path: Path):
    """Default runs dir should resolve under the app XDG data directory."""
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))

    assert get_runs_dir() == tmp_path / "opx-chain" / "runs"


def test_xdg_base_dir_ignores_blank_and_relative_values(monkeypatch):
    """XDG env values must be nonblank absolute paths."""
    monkeypatch.setenv("XDG_DATA_HOME", "   ")
    assert get_data_dir() == Path.home() / ".local" / "share" / "opx-chain"

    monkeypatch.setenv("XDG_DATA_HOME", "relative-data")
    assert get_data_dir() == Path.home() / ".local" / "share" / "opx-chain"


def test_xdg_base_dir_expands_absolute_home_values(monkeypatch, tmp_path: Path):
    """Absolute home-relative values should remain supported after validation."""
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_DATA_HOME", "~/xdg-data")

    assert get_data_dir() == home / "xdg-data" / "opx-chain"


def test_get_runs_dir_uses_storage_dir_override(tmp_path: Path):
    """Configured storage.dir should become the parent of the runs directory."""
    storage_dir = tmp_path / "custom-storage"

    assert get_runs_dir(storage_dir) == storage_dir / "runs"


def test_get_runs_dir_preserves_default_runs_dir_override(tmp_path: Path):
    """Legacy callers can still provide a patched default runs directory."""
    patched_default = tmp_path / "patched-runs"

    assert get_runs_dir(default_runs_dir=patched_default) == patched_default


def test_runtime_runs_dir_callers_delegate_to_shared_helper():
    """Runtime entrypoints should not duplicate storage.dir-to-runs logic."""
    for module_path in (
        ROOT / "opx_chain" / "fetcher.py",
        ROOT / "opx_chain" / "viewer.py",
        ROOT / "opx_chain" / "check_positions.py",
    ):
        source = module_path.read_text(encoding="utf-8")

        assert "get_runs_dir(" in source
        assert 'Path(config.storage_dir) / "runs" if config.storage_dir else' not in source


def test_capture_viewer_screenshot_launches_packaged_viewer():
    """Screenshot tooling should launch the packaged viewer and surface stderr."""
    source = (ROOT / "scripts" / "capture_viewer_screenshot.py").read_text(
        encoding="utf-8"
    )

    assert '[sys.executable, "-m", "opx_chain.viewer"]' in source
    assert "stderr=subprocess.PIPE" in source
    assert "viewer stderr:" in source
    assert "viewer.py" not in source
    assert "stderr=subprocess.DEVNULL" not in source
