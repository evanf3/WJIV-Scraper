from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
import polars as pl
import tempfile
import os
import io
import pandas as pd
from datetime import datetime, timedelta
import regex as re
from analyze_pdf import ReportScraper  # Your existing WJIV PDF scraper

app = FastAPI()

HTML_FORM = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>TMW Tools</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet" />
</head>
<body>
<div class="container" style="max-width:700px; margin-top:50px;">
  <h1 class="mb-4 text-center">PDF Report Scraper</h1>

  <!-- WJIV Form -->
  <div class="mb-5 p-4 border rounded bg-light">
    <h3 class="mb-3">Woodcock Johnson IV Assessment Scraper</h3>
    <form method="post" enctype="multipart/form-data" action="/process_wjiv">
      <div class="mb-3">
        <label for="wjiv_pdfs" class="form-label">Select WJIV PDF files</label>
        <input class="form-control" type="file" id="wjiv_pdfs" name="pdfs" multiple accept=".pdf" required />
      </div>
      <button type="submit" class="btn btn-primary">Process WJIV</button>
    </form>
  </div>

  <!-- SPEAKCAT Form -->
  <div class="p-4 border rounded bg-light">
    <h3 class="mb-3">SPEAKCAT</h3>
    <form method="post" enctype="multipart/form-data" action="/process_speakcat_excel">
      <div class="mb-3">
        <label for="speakcat_excel" class="form-label">Upload SPEAKCAT Excel file</label>
        <input class="form-control" type="file" id="speakcat_excel" name="excel" accept=".xlsx,.xls" required />
      </div>
      <button type="submit" class="btn btn-success">Process SPEAKCAT</button>
    </form>
  </div>
</div>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def get_form():
    return HTML_FORM


@app.post("/process_wjiv", response_class=StreamingResponse)
async def process_wjiv_pdfs(pdfs: list[UploadFile] = File(...)):
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
            df = df.with_columns([
                pl.coalesce([pl.col(col.replace("_right", "")), pl.col(col)]).alias(col.replace("_right", ""))
                for col in right_cols
            ]).drop(right_cols)
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
                "Content-Disposition": 'attachment; filename="wjiv_output.csv"'
            })

    return HTML_FORM


def clean_speakcat_fileobj(fileobj) -> io.BytesIO:
    df = pd.read_excel(fileobj)
    df_str = df.astype(str)

    # Filter rows NOT containing "test" in any email/ID column 
    cols_to_check = [col for col in df.columns if 
                        ("email" in col.lower()) or 
                        ("ID" in col) or
                        ("identifier" in col)
                    ]
    if cols_to_check:
        mask = df[cols_to_check].apply(lambda col: col.astype(str).str.lower().str.contains("test", na=False)).any(axis=1)
        df = df[~mask]
      
    df['submit_timestamp'] = pd.to_datetime(df['submit_timestamp'])
    df.sort_values(by='submit_timestamp', ascending=False, inplace=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        now = datetime.now()
        week_prior = now - timedelta(weeks=1)
        df_last_week = df[df['submit_timestamp'] > week_prior]

        scores_mean = pd.to_numeric(df_last_week['overall_total_score'], errors='coerce').mean()
        avg_score_row = {'Organization': "Average Score", 'overall_total_score': scores_mean}
        study_df = pd.concat([df_last_week, pd.DataFrame([avg_score_row])], ignore_index=True)

        df_last_week.to_excel(writer, sheet_name='Last Week', index=False)

        for study_name in df["StudyID"].unique().tolist():
            study_df = df[df["StudyID"] == study_name]
            scores_mean = pd.to_numeric(study_df['overall_total_score'], errors='coerce').mean()
            avg_score_row = {'Organization': "Average Score", 'overall_total_score': scores_mean}
            study_df = pd.concat([study_df, pd.DataFrame([avg_score_row])], ignore_index=True)

            sheet_name = re.sub(r'[\[\]\:\*\?\/\\]', '', study_name)[:31]
            study_df.to_excel(writer, sheet_name=sheet_name, index=False)

    output.seek(0)
    return output


@app.post("/process_speakcat_excel", response_class=StreamingResponse)
async def process_speakcat_excel(excel: UploadFile = File(...)):
    contents = await excel.read()
    fileobj = io.BytesIO(contents)
    output = clean_speakcat_fileobj(fileobj)

    return StreamingResponse(
        output,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': 'attachment; filename="SPEAKCAT_Results_Cleaned.xlsx"'}
    )
