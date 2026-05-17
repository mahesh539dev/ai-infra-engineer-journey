import os
import asyncio
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


user_input = input("Ask a question: ")
messages = [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user",   "content": user_input}
]


response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=messages,
    temperature=0.7,
    max_tokens=500
)

reply = response.choices[0].message.content
print(reply)