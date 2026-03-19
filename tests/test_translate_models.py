from __future__ import annotations

from app.translate.models import ScenePlannerOutput, SemanticCriticBatchOutput


def test_scene_planner_output_uses_closed_structured_schema() -> None:
    schema = ScenePlannerOutput.model_json_schema(by_alias=True)

    assert schema["additionalProperties"] is False
    who_knows_what = schema["properties"]["who_knows_what"]
    assert who_knows_what["type"] == "array"
    knowledge_ref = who_knows_what["items"]["$ref"].split("/")[-1]
    assert schema["$defs"][knowledge_ref]["additionalProperties"] is False


def test_semantic_critic_output_uses_closed_structured_schema() -> None:
    schema = SemanticCriticBatchOutput.model_json_schema(by_alias=True)

    assert schema["additionalProperties"] is False
    item_ref = schema["properties"]["items"]["items"]["$ref"].split("/")[-1]
    item_schema = schema["$defs"][item_ref]
    assert item_schema["additionalProperties"] is False
    minimal_patch = item_schema["properties"]["minimal_patch"]
    assert minimal_patch["type"] == "array"
    patch_ref = minimal_patch["items"]["$ref"].split("/")[-1]
    assert schema["$defs"][patch_ref]["additionalProperties"] is False
