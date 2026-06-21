"""
Tests for the JSON-flattening logic used inside the `transform` task of
gh_archive_pipeline. The pure parse function is mirrored here from the DAG
file rather than imported, so the DAG remains a single Airflow-deployable
artifact. In a production codebase the parse function would live in a
shared module and be imported by both the DAG and these tests.
"""
import json
from io import StringIO


def parse_events_to_rows(file_obj) -> list[dict]:
    rows = []
    for line in file_obj:
        event = json.loads(line)
        rows.append({
            "event_id":    event.get("id"),
            "event_type":  event.get("type"),
            "actor_id":    event.get("actor", {}).get("id"),
            "actor_login": event.get("actor", {}).get("login"),
            "repo_id":     event.get("repo", {}).get("id"),
            "repo_name":   event.get("repo", {}).get("name"),
            "is_public":   event.get("public"),
            "created_at":  event.get("created_at"),
        })
    return rows


def test_parses_three_events_to_three_rows():
    fake_jsonl = '{"id": "1","type": "PushEvent","actor": {"id": 42, "login": "octocat"},"repo": {"id": 1212, "name": "pipeline1"},"public": true,"created_at": "2024-01-15T14:00:00Z"}\n'
    fake_jsonl += '{"id":"2","type":"WatchEvent","actor":{"id":7,"login":"hubot"},"repo": {"id": 12122, "name": "pipeline12"},"public": true,"created_at": "2024-01-15T14:00:00Z"}\n'
    fake_jsonl += '{"id":"3","type":"DeleteEvent","actor":{"id":8,"login":"hubott"},"repo": {"id": 12123, "name": "pipeline11"},"public": true,"created_at": "2024-01-15T14:00:00Z"}\n'
    file_obj = StringIO(fake_jsonl)
    rows = parse_events_to_rows(file_obj)

    assert len(rows) == 3
    expected_keys = {'event_id', 'event_type', 'actor_id', 'actor_login',
                     'repo_id', 'repo_name', 'is_public', 'created_at'}
    for row in rows:
        assert set(row.keys()) == expected_keys


def test_event_fields_extracted_correctly():
    fake_jsonl = '{"id": "1","type": "PushEvent","actor": {"id": 42, "login": "octocat"},"repo": {"id": 1212, "name": "pipeline1"},"public": true,"created_at": "2024-01-15T14:00:00Z"}\n'
    file_obj = StringIO(fake_jsonl)
    rows = parse_events_to_rows(file_obj)
    assert rows[0]['event_id'] == "1"
    assert rows[0]['event_type'] == 'PushEvent'
    assert rows[0]['actor_id'] == 42
    assert rows[0]['actor_login'] == 'octocat'
    assert rows[0]['repo_id'] == 1212
    assert rows[0]['repo_name'] == 'pipeline1'
    assert rows[0]['is_public'] is True
    assert rows[0]['created_at'] == "2024-01-15T14:00:00Z"


def test_missing_actor_field_handled():
    fake_jsonl = '{"id": "1","type": "PushEvent","repo": {"id": 1212, "name": "pipeline1"},"public": true,"created_at": "2024-01-15T14:00:00Z"}\n'
    file_obj = StringIO(fake_jsonl)
    rows = parse_events_to_rows(file_obj)
    assert rows[0]['actor_id'] is None
    assert rows[0]['actor_login'] is None


def test_handles_empty_input():
    file_obj = StringIO("")
    rows = parse_events_to_rows(file_obj)
    assert len(rows) == 0
