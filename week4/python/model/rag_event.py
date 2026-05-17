from pydantic import BaseModel
from datetime import datetime


class DocumentModel(BaseModel):
    document_id: str
    document_name: str
    document_type: str
    source_path: str


class ChunkModel(BaseModel):
    chunk_id: str
    chunk_index: int
    total_chunks: int
    text: str
    character_count: int


class ProcessingModel(BaseModel):
    chunking_strategy: str
    chunk_size: int
    chunk_overlap: int


class PineconeModel(BaseModel):
    index_name: str
    namespace: str


class MetadataModel(BaseModel):
    tags: list[str]
    language: str


class RagEvent(BaseModel):
    event_id: str
    event_type: str
    event_version: str
    timestamp: datetime
    request_id: str
    trace_id: str
    document: DocumentModel
    chunk: ChunkModel
    processing: ProcessingModel
    pinecone: PineconeModel
    metadata: MetadataModel

    @property
    def text(self) -> str:
        return self.chunk.text
