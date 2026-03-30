import io
import os
import tempfile
from fastapi import FastAPI, UploadFile, File
from import_csv import import_csv

app = FastAPI()

@app.post("/processar")
async def processar(file: UploadFile = File(...)):
    contents = await file.read()
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(contents)
        tmp_path = tmp.name
    
    try:
        import_csv(tmp_path)
        return {"status": "ok", "arquivo": file.filename}
    except Exception as e:
        return {"status": "erro", "detalhe": str(e)}
    finally:
        os.unlink(tmp_path)
