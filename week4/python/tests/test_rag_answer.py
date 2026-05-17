import pytest
from unittest.mock import MagicMock, patch


SAMPLE_RESULTS = [
    {"metadata": {"message": "Kafka is a distributed event streaming platform."}},
    {"metadata": {"message": "Pinecone is a vector database for similarity search."}},
]


@patch("llm.rag_answer.client")
def test_generate_answer_calls_openai_with_context(mock_client):
    from llm.rag_answer import generate_answer

    mock_response = MagicMock()
    mock_response.choices[0].message.content = "Kafka streams events; Pinecone stores vectors."
    mock_client.chat.completions.create.return_value = mock_response

    result = generate_answer("What is Kafka?", SAMPLE_RESULTS)

    assert result == "Kafka streams events; Pinecone stores vectors."
    call_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert call_kwargs["model"] == "gpt-4o-mini"
    messages = call_kwargs["messages"]
    assert messages[0]["role"] == "system"
    assert "context" in messages[1]["content"].lower()
    assert "What is Kafka?" in messages[1]["content"]
    assert "Kafka is a distributed event streaming platform." in messages[1]["content"]


@patch("llm.rag_answer.client")
def test_generate_answer_returns_fallback_when_no_results(mock_client):
    from llm.rag_answer import generate_answer

    result = generate_answer("What is Kafka?", [])

    mock_client.chat.completions.create.assert_not_called()
    assert result == "I could not find relevant information to answer your question."
