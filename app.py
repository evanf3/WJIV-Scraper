from fastapi import FastAPI, File, UploadFile, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import polars as pl
import tempfile
import os
from analyze_pdf import ReportScraper
import io

app = FastAPI()
templates = Jinja2Templates(directory="templates")  # you can skip and do inline if you want

HTML_FORM = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Woodcock Johnson IV Report Scraper</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet" />
</head>
<body>
<div class="container" style="max-width:600px; margin-top:50px; background:#fff; padding:30px; border-radius:10px;">
  <h1 class="mb-4 text-center">Upload Woodcock Johnson IV Reports</h1>
  <form method="post" enctype="multipart/form-data">
    <div class="mb-3">
      <label for="pdfs" class="form-label">Select PDF files (multiple allowed)</label>
      <input class="form-control" type="file" id="pdfs" name="pdfs" multiple accept=".pdf" required />
    </div>
    <button type="submit" class="btn btn-primary w-100">Process PDFs</button>
  </form>
</div>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def get_form():
    return HTML_FORM

@app.post("/", response_class=StreamingResponse)
async def upload_pdfs(pdfs: list[UploadFile] = File(...)):
    english_files = []
    spanish_files = []

    with tempfile.TemporaryDirectory() as tmpdirname:
        for upload in pdfs:
            file_path = os.path.join(tmpdirname, upload.filename)
            contents = await upload.read()
            with open(file_path, "wb") as f:
                f.write(contents)
            r = ReportScraper(path=file_path)
            r.get_headers()
            r.set_id(id_key="Name")
            r.get_test_scores()
            r.get_observations()
            if r.language == "English":
                english_files.append(r.data)
            else:
                spanish_files.append(r.data)

        en_df = pl.DataFrame(english_files) if english_files else pl.DataFrame()
        sp_df = pl.DataFrame(spanish_files) if spanish_files else pl.DataFrame()

        if not en_df.is_empty() and not sp_df.is_empty():
            df = en_df.join(sp_df, on="ID", how="full", coalesce=True)

            right_cols = [col for col in df.columns if col.endswith("_right")]
            replacements = [
                pl.coalesce([pl.col(col.replace("_right", "")), pl.col(col)]).alias(col.replace("_right", ""))
                for col in right_cols
            ]
            df = df.with_columns(replacements)
            df = df.drop(right_cols)
        elif not en_df.is_empty():
            df = en_df
        elif not sp_df.is_empty():
            df = sp_df
        else:
            df = pl.DataFrame()

        if not df.is_empty():
            df = df.select(pl.col("ID"), pl.all().exclude("ID"))
            df = df.unique().sort("ID")

            csv_bytes = df.write_csv().encode("utf-8")
            return StreamingResponse(io.BytesIO(csv_bytes), media_type="text/csv", headers={
                "Content-Disposition": 'attachment; filename="output.csv"'
            })

    return HTML_FORM  # fallback - if no data

# To run: uvicorn app:app --reload
