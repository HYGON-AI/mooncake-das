"""Check the label-only HCU entry points. Run with Python and PyYAML installed."""

from pathlib import Path

import yaml


def test_label_triggers():
    workflows = Path(__file__).resolve().parents[2] / ".github" / "workflows"
    for name, event, branch in (
        ("ci_hygon_validation.yml", "pull_request", "official_code_ci"),
        ("ci_hygon_pr.yml", "pull_request_target", "main"),
    ):
        data = yaml.load((workflows / name).read_text(), Loader=yaml.BaseLoader)
        assert data["on"] == {event: {"branches": [branch], "types": ["labeled"]}}
        job = data["jobs"]["hcu"]
        assert "github.event.label.name == 'run-hygon-ci'" in job["if"]
        assert "github.event.pull_request.head.repo.full_name == github.repository" in job["if"]
        assert job["with"]["checkout_ref"] == "${{ github.event.pull_request.head.sha }}"
        assert data["permissions"] == {"contents": "read"}
        cleanup = data["jobs"]["cleanup-label"]
        assert cleanup["runs-on"] == "ubuntu-latest"
        assert cleanup["permissions"] == {"pull-requests": "write"}
        assert not any("uses" in step for step in cleanup["steps"])
    print("HCU label trigger checks passed")


if __name__ == "__main__":
    test_label_triggers()
