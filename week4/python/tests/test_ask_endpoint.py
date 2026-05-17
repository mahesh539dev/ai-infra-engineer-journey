import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient


SAMPLE_PINECONE_RESULTS = [
    {"metadata": {"message": "Kafka is a distributed event streaming platform."}},
]


@pytest.fixture()
def client():
    with patch("api.data_apis.EmbeddingService"), \
         patch("api.data_apis.generate_answer"):
        from api.data_apis import app
        return TestClient(app)


def test_ask_returns_answer(client):
    with patch("api.data_apis.embedding_service") as mock_svc, \
         patch("api.data_apis.generate_answer") as mock_gen:
        mock_svc.semantic_search_with_query.return_value = SAMPLE_PINECONE_RESULTS
        mock_gen.return_value = "Kafka streams events."

        response = client.get("/ask", params={"query": "What is Kafka?"})

    assert response.status_code == 200
    assert response.json() == {"answer": "Kafka streams events."}
    mock_svc.semantic_search_with_query.assert_called_once_with("What is Kafka?")
    mock_gen.assert_called_once_with("What is Kafka?", SAMPLE_PINECONE_RESULTS)


def test_ask_returns_404_when_no_pinecone_results(client):
    with patch("api.data_apis.embedding_service") as mock_svc, \
         patch("api.data_apis.generate_answer") as mock_gen:
        mock_svc.semantic_search_with_query.return_value = None

        response = client.get("/ask", params={"query": "What is Kafka?"})

    assert response.status_code == 404
    assert response.json() == {"message": "No results found"}
    mock_gen.assert_not_called()


def test_ask_returns_422_when_query_missing(client):
    response = client.get("/ask")
    assert response.status_code == 422
