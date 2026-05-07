from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from openai import OpenAI

from .models import AiSuggestResponse


def _openai_model() -> str:
    return os.environ.get("OPENAI_MODEL", "gpt-4o-mini")


def _openai_client() -> OpenAI:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not configured")
    return OpenAI(api_key=api_key)


def suggest_transformations(
    prompt: str,
    transformations: List[Dict[str, Any]],
    columns: List[str],
    sample_rows: List[Dict[str, Any]],
) -> AiSuggestResponse:
    """
    Suggest transformation steps from natural language.
    Returns a validated AiSuggestResponse model.
    """
    client = _openai_client()
    system_msg = (
        "You are a data transformation assistant. "
        "Generate ONLY valid transformation suggestions using the provided transformation types "
        "and parameter constraints. If the request is ambiguous, set needsClarification=true and "
        "ask one concise question. Never invent columns not present in the schema."
    )
    user_payload = {
        "task": prompt,
        "allowed_transformations": transformations,
        "available_columns": columns,
        "sample_rows": sample_rows,
        "response_contract": {
            "steps": [{"type": "string", "parameters": "object"}],
            "explanation": "string",
            "assumptions": ["string"],
            "needsClarification": "boolean",
            "clarificationQuestion": "string|null",
        },
    }

    completion = client.chat.completions.create(
        model=_openai_model(),
        temperature=0.1,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": json.dumps(user_payload)},
        ],
    )
    content = completion.choices[0].message.content or "{}"
    parsed = json.loads(content)
    return AiSuggestResponse.model_validate(parsed)
