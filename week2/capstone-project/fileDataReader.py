import pypdf

def extract_pdf_text(pdf_path):
    text_by_page = []
    with open(pdf_path,"rb") as f:
        reader = pypdf.PdfReader(f)
        for i, page in enumerate(reader.pages):
            text = page.extract_text()
            if text and text.strip():
                text_by_page.append({"page": i + 1, "text": text})
    return text_by_page

def chunk_text(text, chunk_size=1000, overlap=100):
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start+=chunk_size-overlap
    return chunks

def extract_pdf_text_in_chunks_with_metadat(pdf_path):
    all_chunks = []
    all_ids = []
    all_metadata = []
    
    all_pages_data = extract_pdf_text(pdf_path)
    
    for page_data in all_pages_data:
        chunks = chunk_text(page_data["text"])
        for j,chunk in enumerate(chunks):
            chunk_id = f"page{page_data['page']}_chunk{j}"
            all_chunks.append(chunk)
            all_ids.append(chunk_id)
            all_metadata.append({"page": page_data["page"], "chunk": j})
    pdfChunkData = {
        "ids": all_ids,
        "text": all_chunks,
        "metadatas": all_metadata
    }
    
    return pdfChunkData