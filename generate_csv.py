from analyze_pdf import ReportScraper
import os  
import polars as pl 

english_files = [] 
spanish_files = []

for file in os.scandir("data"):  
    if file.is_file():  # check if it's a file
        r = ReportScraper(path=file.path)
        r.get_headers()
        r.set_id(id_key="Name") # name" in report is actually an id
        r.get_test_scores()
        r.get_observations()
        
        if r.language == "English":
            english_files.append(r.data)
        else:
            spanish_files.append(r.data)

# Make dataframes from english and spanish lists of data dicts
en_df = pl.DataFrame(english_files) 
sp_df = pl.DataFrame(spanish_files) 

# Merge english and spanish df's 
df = en_df.join(sp_df, on="ID", how="full", coalesce=True)

# Coalesce duplicate header information (stored as *_right columns)
right_cols = [col for col in df.columns if col.endswith("_right")]

# Right column replacement expressions
replacements = [
    pl.coalesce([pl.col(col.replace("_right", "")), pl.col(col)]).alias(col.replace("_right", ""))
    for col in right_cols
]
# Apply replacements
df = df.with_columns(replacements)

# Drop the duplicate columns
df = df.drop(right_cols)

# Move id column to front 
df = df.select(pl.col("ID"), pl.all().exclude("ID"))

# Sort, Print, Save
df = df.unique().sort(["ID"])
print(df)
df.write_csv("output.csv", separator=",")
