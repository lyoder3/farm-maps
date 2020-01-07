import csv
import itertools
import os
import re
from collections import defaultdict

import geopandas
import gspread_pandas
import numpy as np
import pandas as pd

year = 2019

secret_filepath = r"F:\Farm\FarmDataAutomation"
secret_filename = "SMS_secret.json"
working_dir = r"F:\Farm\FarmDataAutomation\CropPlans"
os.chdir(working_dir)


def clean_master_sheet(client):
    df = client.sheet_to_df(sheet=0, header_rows=1, index=0)
    cols = df.columns.to_list()
    new_cols = cols[cols.index("Farm Name") : cols.index("Field") + 1]
    ix = (cols[cols.index(f"{year + 1} SPRING")], cols[cols.index(f"{year} FALL")])
    new_cols.extend(ix)
    df = df[new_cols]
    df.rename(
        columns={
            "Farm Name": "Farm",
            f"{year + 1} SPRING": f"SPRING{year + 1}",
            f"{year} FALL": f"FALL{year}",
        },
        inplace=True,
    )
    df = df[
        (~df[f"SPRING{year + 1}"].isna())
        & (~df[f"SPRING{year+1}"].isin(["CREP", "P01"]))
    ]
    df.fillna("None", inplace=True)
    df = df.replace({"": "None"})
    df = df.loc[(df.iloc[:, 2] != "None") | (df.iloc[:, 3] != "None"), :]
    df["GROWER"] = df.apply(
        lambda row: re.match(r"[HJS][OCRZ]", row.Farm).group(0), axis=1
    )
    df = df.set_index(["Farm", "Field"])

    df = df.replace(
        [
            "CC BARLEY",
            "BARLEY/CLOVER",
            "BARLEY/CLOVER/RADISH",
            "CC OATS",
            "BARLEY/DOUBLE CROP BEANS",
            "WHEAT/BARLEY/RADISH",
        ],
        "COVER CROP",
    )
    return df

def write_csvs(df):

    spring_fields = [
        (row[0][0], row[0][1], row[1], row[3], f"{year + 1}", "Planting - Spring")
        for row in df.itertuples()
        if row[1] != "None"
    ]

    fall_fields = [
        (row[0][0], row[0][1], row[2], row[3], f"{year}", "Planting - Autumn")
        for row in df.itertuples()
        if row[2] != "None"
    ]


    for i in spring_fields:
        file_name = i[0] + "_" + i[1] + "_" + f"{year + 1}SPRING" + ".csv"
        with open(file_name, "w+", newline="") as out:
            file_writer = csv.writer(out)
            file_writer.writerow(
                ["FARM", "FIELD_1", "PRODUCT_1", "GROWER", "YEAR", "SEASON"]
            )
            file_writer.writerow(i)
    for i in fall_fields:
        file_name = i[0] + "_" + i[1] + "_" + f"{year}FALL" + ".csv"
        with open(file_name, "w+", newline="") as out:
            file_writer = csv.writer(out)
            file_writer.writerow(
                ["FARM", "FIELD_1", "PRODUCT_1", "GROWER", "YEAR", "SEASON"]
            )
            file_writer.writerow(i)

def sort_boundaries():
    shp_dict = defaultdict(list)
    for f in os.listdir(f"{secret_filepath}\\boundaries"):
        f = os.path.join(f"{secret_filepath}\\boundaries", f)
        m = re.search(r"([HJS][OCR][A-Z\W]+)(?:_)([CP]\d{2})(?=_\d{4}|_NO Year)", f,)
        if m:
            tag = m.group(1) + "_" + m.group(2)
            if f.endswith((".shp", ".shx", ".dbf", ".prj")):
                shp_dict[tag].append(f)
        else:
            print(f)
    return shp_dict

def check_fields_to_file(csvs, shapes):
    csvs = csvs.keys()
    shps = shapes.keys()

    no_boundary = [i for i in csvs if i not in shps]
    no_cropplans = [i for i in shps if i not in csvs]
    
    with open('No Boundary.txt', 'w+') as f:
        for i in no_boundary:
            f.write(f'{i}\n')
    with open('No Planned Crops.txt', 'w+') as f:
        for i in no_cropplans:
            f.write(f'{i}\n')
    for k,v in shapes.items():
        if k in no_cropplans:
            for f in v:
                os.remove(f)

def mergedfs(geodf, df):
    geodf["tmp"] = 1
    df["tmp"] = 1
    return pd.merge(geodf, df, on="tmp")

def make_cropplans_chunks(csvs, shapes):
    max_folder_size = 500
    import_size = len(list(itertools.chain.from_iterable(csvs.values())))
    folders = int(np.ceil(import_size / max_folder_size))
    n = int(5 * np.ceil((import_size / folders) / 5))
    csv1 = [(k, v[0]) for k, v in csvs.items() if len(v) > 1]
    csv2 = [(k, v[1]) for k, v in csvs.items() if len(v) > 1]
    csv3 = [(k, v) for k, v in csvs.items() if len(v) == 1]
    csvs = csv1 + csv2 + csv3
    csvs = [csvs[i * n : (i + 1) * n] for i in range((len(csvs) + n - 1) // n)]
    for i in csvs:
        n = csvs.index(i) + 1
        os.makedirs(f"Crop Plans {n}", exist_ok=True)
        for x in i:
            if isinstance(x[1], list):
                end_dir = os.path.join(os.getcwd(), f"Crop Plans {n}")
                file_name = os.path.basename(x[1][0])
                name, ext = os.path.splitext(file_name)
                out_file = os.path.join(end_dir, f"Crop Plans {name}.shp")
                df = pd.read_csv(x[1][0])
                for k1, v1 in shapes.items():
                    if x[0] == k1 and v1:
                        for f in v1:
                            if f.endswith(".shp"):
                                geodf = geopandas.read_file(f)
                mergedfs(geodf, df).to_file(out_file)
            else:
                end_dir = os.path.join(os.getcwd(), f"Crop Plans {n}")
                file_name = os.path.basename(x[1])
                name, ext = os.path.splitext(file_name)
                out_file = os.path.join(end_dir, f"Crop Plans {name}.shp")
                df = pd.read_csv(x[1])
                for k1, v1 in shapes.items():
                    if x[0] == k1 and v1:
                        for f in v1:
                            if f.endswith(".shp"):
                                geodf = geopandas.read_file(f)
                mergedfs(geodf, df).to_file(out_file)


def main():
    credentials = gspread_pandas.conf.get_creds(
    config=gspread_pandas.conf.get_config(
        conf_dir=secret_filepath, file_name=secret_filename
    ),
    creds_dir=secret_filepath,)

    client = gspread_pandas.spread.Spread(spread="SMI Master Field Sheet", creds=credentials)

    df = clean_master_sheet(client)

    """Dictionary to replace planned crops with standardized names for SMS
            You can add more by inserting a comma then writing a new key:value pair.
            The key is before the colon and is what you are trying to replace.
            The value is after the colon and that is what you are replacing every instance of the key with.
            e.g. in the code below "CC BARLEY" gets replaced with "BARLEY" """

    df = df.replace(
        [
            "CC BARLEY",
            "BARLEY/CLOVER",
            "BARLEY/CLOVER/RADISH",
            "CC OATS",
            "BARLEY/DOUBLE CROP BEANS",
            "WHEAT/BARLEY/RADISH",
        ],
        "COVER CROP",
    )

    # Always creating crop plans for one year ahead of current year
    """can put the directory where you have your boundary files at, but I usually just copy the script to that directory
        then open the script there along with the folder in VSCode that way you don't have to 
        fumble around with setting the working directory"""

    # Writes csv files for each field

    write_csvs(df)

    shp_dict = sort_boundaries()

    csv_dict = defaultdict(list)

    for f in os.listdir(working_dir):
        m = re.search(
            r"([HJS][OCR][A-Z\W]+)(?:_)([CP]\d{2}|WHOLE FARM|ALL)(?=_\d{4}|_NO Year)",
            f,
        )
        if m:
            tag = m.group(1) + "_" + m.group(2)
            if f.endswith(".csv"):
                csv_dict[tag].append(f)

    # Removes the fields where there is no crop plan csv
    # # # joins the crop plans with the boundary shape file
    check_fields_to_file(csv_dict, shp_dict)
    make_cropplans_chunks(csv_dict, shp_dict)


if __name__ == "__main__":
    main()
