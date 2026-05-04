from pydantic import BaseModel


class KafkaEvent(BaseModel):
    message: str
    topic: str
    subtopic: str