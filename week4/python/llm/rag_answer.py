import os
from dotenv import load_dotenv
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import openai
import logging 

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are a helpful assistant. Answer the user's question using only the "
    "context passages provided below. If the context does not contain enough "
    "information to answer, say \"I don't have enough information to answer that.\""
)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=1, max=5),
    retry=retry_if_exception(lambda e: isinstance(e, openai.APIConnectionError))
)
def generate_answer(query: str, pinecone_results: list[dict]) -> str:
    try:
        if not pinecone_results:
            return "I could not find relevant information to answer your question."

        context_passages = "\n".join(
            f"{i + 1}. {match['metadata']['text']}"
            for i, match in enumerate(pinecone_results)
        )
        user_message = f"Context:\n{context_passages}\n\nQuestion: {query}"

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0.3,
            max_tokens=500,
        )
        return response.choices[0].message.content or ""
    except openai.APIConnectionError as e:
        logger.error("API connection is not working", extra={"message": str(e)}, exc_info=True)
    except Exception as e:
        logger.error("Error while calling llm", extra={"error": str(e)}, exc_info=True)
        