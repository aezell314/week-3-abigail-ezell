"""Day 3 checkpoint: orchestration runs offline with recorded stage calls."""

from de_pipeline import pipeline


def test_main_runs_stages_in_order_and_prints_counts(monkeypatch, capsys):
    events = []

    class Connection:
        def close(self):
            events.append("close")

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

    con = Connection()

    def ingest():
        events.append("ingest")
        return 6

    def fetch_all():
        events.append("fetch")
        return {"characters": "data/raw/characters.json"}

    def connect():
        events.append("connect")
        return con

    def load_all(connection):
        assert connection is con
        events.append("load")
        return {"raw_characters": 6}

    def run_transforms(connection):
        assert connection is con
        events.append("transform")
        return {"clean_characters": 5, "species_summary": 2, "episode_appearances": 3}

    monkeypatch.setattr(pipeline.api, "ingest", ingest)
    monkeypatch.setattr(pipeline.fetch, "fetch_all", fetch_all)
    monkeypatch.setattr(pipeline.load, "connect", connect)
    monkeypatch.setattr(pipeline.load, "load_all", load_all)
    monkeypatch.setattr(pipeline.transform, "run_transforms", run_transforms)

    pipeline.main()

    assert events == ["ingest", "fetch", "connect", "load", "transform", "close"]
    output = capsys.readouterr().out
    for value in ("6", "raw_characters", "clean_characters", "5", "species_summary",
                  "2", "episode_appearances", "3"):
        assert value in output
