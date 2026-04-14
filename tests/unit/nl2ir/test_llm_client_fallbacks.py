from server.nl2ir.llm_client import LLMClient


def _make_client():
    return object.__new__(LLMClient)


def test_extract_function_call_falls_back_to_plain_json_content():
    client = _make_client()
    response = {
        "choices": [
            {
                "message": {
                    "content": '{"arguments": {"metric": "amount", "group_by": ["region"]}}'
                }
            }
        ]
    }

    result = client.extract_function_call(response)

    assert result == {"metric": "amount", "group_by": ["region"]}


def test_extract_function_call_handles_function_wrapper_in_content():
    client = _make_client()
    response = {
        "choices": [
            {
                "message": {
                    "content": '{"function": {"name": "produce_ir", "arguments": "{\\"limit\\": 10}"}}'
                }
            }
        ]
    }

    result = client.extract_function_call(response)

    assert result == {"limit": 10}


def test_remove_enable_thinking_strips_nested_fields():
    request_params = {
        "extra_body": {
            "enable_thinking": True,
            "chat_template_kwargs": {
                "enable_thinking": True,
                "other_flag": "kept",
            },
        }
    }

    removed = LLMClient._remove_enable_thinking(request_params)

    assert removed is True
    assert request_params == {
        "extra_body": {
            "chat_template_kwargs": {
                "other_flag": "kept",
            }
        }
    }


def test_remove_tooling_removes_tools_and_tool_choice():
    request_params = {
        "tools": [{"type": "function"}],
        "tool_choice": {"type": "function"},
        "model": "demo-model",
    }

    removed = LLMClient._remove_tooling(request_params)

    assert removed is True
    assert request_params == {"model": "demo-model"}
