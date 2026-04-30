import uvicorn
from datasearchapi import app
from chroma_actions import load_data_to_chroma

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
