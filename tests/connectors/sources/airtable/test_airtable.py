from types import SimpleNamespace

from bizon.connectors.sources.airtable.src.source import AirtableSource


def _shape(fields, raw_fields, ai_text_as_string=False):
    rec = {"id": "rec1", "createdTime": "2026-01-01T00:00:00.000Z", "fields": raw_fields}
    stub = SimpleNamespace(config=SimpleNamespace(ai_text_as_string=ai_text_as_string))
    return AirtableSource._shape_record(stub, rec, fields, "Table")


def test_button_and_attachment_values_become_python_reprs() -> None:
    """Both arrive from the API as list/dict but map to STRING; JSON_VALUE on a nested object yields NULL."""
    shaped = _shape(
        [
            ("Cleaner", "button", "cleaner"),
            ("Logo", "multipleAttachments", "logo"),
        ],
        {
            "Cleaner": {"label": "Button", "url": "https://airtable.com/tbl1/rec1"},
            "Logo": [{"id": "att1", "url": "https://example.test/signed"}],
        },
    )

    assert shaped["cleaner"] == "{'label': 'Button', 'url': 'https://airtable.com/tbl1/rec1'}"
    assert shaped["logo"] == "[{'id': 'att1', 'url': 'https://example.test/signed'}]"


def test_scalar_and_json_typed_fields_are_left_alone() -> None:
    shaped = _shape(
        [
            ("Name", "singleLineText", "name"),
            ("Tags", "multipleSelects", "tags"),
        ],
        {"Name": "Olark", "Tags": ["a", "b"]},
    )

    assert shaped["name"] == "Olark"
    assert shaped["tags"] == ["a", "b"]


def test_absent_field_stays_none_rather_than_the_string_none() -> None:
    shaped = _shape([("Cleaner", "button", "cleaner")], {})

    assert shaped["cleaner"] is None


def test_user_object_types_become_reprs() -> None:
    user = {"id": "usr1", "email": "a@b.test", "name": "A"}
    shaped = _shape(
        [
            ("Created By", "createdBy", "created_by"),
            ("Edited By", "lastModifiedBy", "edited_by"),
            ("Owner", "singleCollaborator", "owner"),
        ],
        {"Created By": user, "Edited By": user, "Owner": user},
    )

    expected = "{'id': 'usr1', 'email': 'a@b.test', 'name': 'A'}"
    assert shaped["created_by"] == expected
    assert shaped["edited_by"] == expected
    assert shaped["owner"] == expected


def test_ai_text_is_stringified_only_when_the_stream_opts_in() -> None:
    fields = [("Summary", "aiText", "summary")]
    raw = {"Summary": {"state": "generated", "value": "hi", "isStale": False}}

    assert _shape(fields, raw)["summary"] == raw["Summary"]
    assert _shape(fields, raw, ai_text_as_string=True)["summary"] == (
        "{'state': 'generated', 'value': 'hi', 'isStale': False}"
    )
