import pandas as pd
import os


def generate_visualizations():
    os.makedirs("visualizations", exist_ok=True)
    try:
        # viz 1
        df = pd.read_parquet("./data/case/gold/characters/characters_participation.parquet", engine="pyarrow")
        with open("./visualizations/characters_participation.html", 'w') as f:
                f.write(df.to_html())

        # viz 2
        df = pd.read_parquet("./data/case/gold/characters/characters_count_each_year.parquet", engine="pyarrow")
        with open("./visualizations/characters_count_each_year.html", 'w') as f:
                f.write(df.to_html())


        #viz 3
        df = pd.read_parquet("./data/case/gold/characters/total_years_top10_appeared.parquet", engine="pyarrow")
        with open("./visualizations/total_years_top10_appeared.html", 'w') as f:
                f.write(df.to_html())
    except FileNotFoundError as e:
        print(e)
        
generate_visualizations()