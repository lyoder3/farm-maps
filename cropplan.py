import csv
import datetime
import glob
import logging
import os
import re
import shutil
import time
from collections import defaultdict
from multiprocessing import Pool, log_to_stderr

import geopandas
import gspread_pandas
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.DEBUG, filename='log.txt',
                    format='%(asctime)s:%(levelname)s:%(message)s')
# type the year you want to do crop plans for here
# Example: 2019 would give crop plans for FALL 19 and SPRING 20
year = 2019

secret_filepath = r'F:\Farm\FarmDataAutomation'
secret_filename = 'SMS_secret.json'


def mergedfs(geodf, df):
    geodf["tmp"] = 1
    df["tmp"] = 1
    return pd.merge(geodf, df, on="tmp")


def sort_farm_field():
    logging.info('Beginning to sort by farm and field.')
    for file in glob.iglob("*"):
        m = re.search(
            r"([HJS][OCR][A-Z\W]+)(?:_)([CP]\d{2}|WHOLE FARM|ALL)(?=_\d{4}|_NO Year)",
            file,
        )
        if m:
            tag = m.group(1) + "_" + m.group(2)
            yield tag, file
    logging.info(f'Completed sorting by farm and field.')


def clean_master_sheet():
    df = client.sheet_to_df(sheet=0, header_rows=1, index=0)
    cols = df.columns.to_list()
    new_cols = cols[cols.index("Farm Name"): cols.index("Field") + 1]
    ix = (cols[cols.index(f"{year + 1} SPRING")],
          cols[cols.index(f"{year} FALL")])
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
    return df


def write_csvs(df):
    logging.info('Starting to write CSVs.')

    spring_fields = [
        (row[0][0], row[0][1], row[1], row[3],
         f"{year + 1}", 'Planting - Spring')
        for row in df.itertuples()
        if row[1] != "None"
    ]
    logging.debug(f'The last spring crop was {spring_fields[-1]}.')

    fall_fields = [
        (row[0][0], row[0][1], row[2], row[3], f"{year}", "Planting - Autumn")
        for row in df.itertuples()
        if row[2] != "None"
    ]

    logging.debug(f'The last fall crop was {fall_fields[-1]}.')

    for i in spring_fields:
        file_name = i[0] + "_" + i[1] + "_" + f"{year + 1}SPRING" + ".csv"
        with open(file_name, "w+", newline="") as out:
            file_writer = csv.writer(out)
            file_writer.writerow(
                ["FARM", "FIELD", "PRODUCT", "GROWER", "YEAR", "SEASON"]
            )
            file_writer.writerow(i)
    for i in fall_fields:
        file_name = i[0] + "_" + i[1] + "_" + f"{year}FALL" + ".csv"
        with open(file_name, "w+", newline="") as out:
            file_writer = csv.writer(out)
            file_writer.writerow(
                ["FARM", "FIELD", "PRODUCT", "GROWER", "YEAR", "SEASON"]
            )
            file_writer.writerow(i)
    logging.info('Finished writing CSVs.')


def make_dfs(fields):
    fall = [field for field in fields if any(
        f.endswith("FALL.csv") for f in field)]
    print(len(fall))
    spring = [
        field
        for field in fields
        if any(f.endswith("SPRING.csv") for f in field) and field not in fall
    ]
    print(len(spring))

    for field in fall:
        for f in field:
            if f.endswith("FALL.csv"):
                fdf = pd.read_csv(f)
                dum = os.path.basename(f)
                d, ext = os.path.splitext(dum)
                fout = os.path.join("CROP PLANS", f"CROP PLANS {d}.shp")
                print(fout)
            elif f.endswith("SPRING.csv"):
                sdf = pd.read_csv(f)
                dum = os.path.basename(f)
                d, ext = os.path.splitext(dum)
                sout = os.path.join("CROP PLANS", f"CROP PLANS {d}.shp")
                print(sout)
            elif f.endswith(".shp"):
                geodf = geopandas.read_file(f)
                print(geodf)
        mergedfs(geodf, fdf).to_file(fout)
        mergedfs(geodf, sdf).to_file(sout)
    for field in spring:
        for f in field:
            if f.endswith(".csv"):
                sdf = pd.read_csv(f)
                dum = os.path.basename(f)
                d, ext = os.path.splitext(dum)
                sout = os.path.join("CROP PLANS", f"CROP PLANS {d}.shp")
            elif f.endswith(".shp"):
                geodf = geopandas.read_file(f)
        mergedfs(geodf, sdf).to_file(sout)


def delete_extra_boundaries(fields):
    logging.info('Beginning to remove fields with no boundary files.')
    i = 0
    with open('No Planned Crops.txt', 'w+', newline='\n') as f:
        for field in fields:
            if any(i.endswith(".csv") for i in field):
                continue
            else:
                f.write("%s\n" % field[0])
                for a in field:
                    os.remove(a)
                i += 1
    logging.info(f'Completed removing {i} fields without planned crops.')

    # os.startfile('No Planned Crops.txt')

    return fields


def get_noboundary_files(fields):
    logging.info(
        'Beginning to find fields with planned crops and no boundaries.')

    bad = []
    no_boundary = [field for field in fields if all(
        f.endswith(".csv") for f in field)]
    fields = [field for field in fields if any(
        f.endswith(".shp") for f in field)]

    rand_int = np.random.randint(0, 326)

    logging.debug(f'There are {len(no_boundary)} fields without boundaries')

    logging.info(f'Example of a "good" field: {fields[rand_int]}')

    for field in no_boundary:
        name = re.search(r"[A-Z_\W\d]+(?=_\d{4})", field[0]).group(0)
        bad.append(name)
    with open("NEED BOUNDARY.txt", "w+", newline="\n") as f:
        for i in bad:
            f.write("%s\n" % i)
    # os.startfile("NEED BOUNDARY.txt")
    return fields

def sort_chunks(directory):
    files = os.listdir(directory)
    import_size = len(files)
    print(import_size)
    max_folder_size = 2500
    folders = np.ceil(import_size/max_folder_size)
    n = np.int(5 * np.ceil((import_size / folders)/5))
    files = [files[i * n:(i+1)*n] for i in range((len(files) + n -1) // n)]
    g = 1
    for i in files:
        try:
            os.mkdir(os.path.join(par_dir,f'Crop Plans {g}'))
            new_dir = os.path.join(par_dir, f'Crop Plans {g}')
        except FileExistsError:
            pass
        for f in i:
            os.rename(os.path.join(directory,f), os.path.join(new_dir, f))
        g+=1

def main():
    # Always creating crop plans for one year ahead of current year
    """can put the directory where you have your boundary files at, but I usually just copy the script to that directory
        then open the script there along with the folder in VSCode that way you don't have to 
        fumble around with setting the working directory"""

    # Writes csv files for each field

    # write_csvs(df)

    # # Sorts the boundary shape files and new crop planning csv files into folders
    # # Follows structure: 'WorkingDirectory\\FARM\\FIELD\\files'

    fields = sort_farm_field()

    d = {}
    for x, y in fields:
        d.setdefault(x, []).append(y)

    fields = list(d.values())

    random_int = np.random.randint(0, 223)

    logging.info(
        f'Completed sorting by field. Sample field: {fields[random_int]}')

    # Removes the fields where there is no crop plan csv
    fields = delete_extra_boundaries(fields)
    # # joins the crop plans with the boundary shape files
    fields = get_noboundary_files(fields)

    logging.debug(
        f'There are {len(fields)} fields to be made into crop plans.')

    logging.debug(f'{fields[:50]}')

    try:
        os.mkdir("CROP PLANS")
    except FileExistsError:
        pass

    csvs_dict = defaultdict(list)
    shp_dict = defaultdict(list)
    for field in fields:
        for f in field:
            m = re.search(
                r"([HJS][OCR][A-Z\W]+)(?:_)([CP]\d{2}|WHOLE FARM|ALL)(?=_\d{4}|_NO Year)",
                f,
            )
            tag = m.group(1) + ' ' + m.group(2)

            if f.endswith('.csv'):
                csvs_dict[tag].append(f)
            else:
                shp_dict[tag].append(f)

    for k, v in csvs_dict.items():
        for i in v:
            file_name = os.path.basename(i)
            name, ext = os.path.splitext(file_name)
            out_file = os.path.join('CROP PLANS', f'CROP PLANS {name}.shp')
            df = pd.read_csv(i)
            for key, value in shp_dict.items():
                if k == key:
                    for f in value:
                        if f.endswith('.shp'):
                            geodf = geopandas.read_file(f)
            mergedfs(geodf, df).to_file(out_file)

    # # # # # Makes the CROP PLAN directory, sort files into this directory, then splits into chunks to go back to SMS



if __name__ == "__main__":
    credentials = gspread_pandas.conf.get_creds(config=gspread_pandas.conf.get_config(conf_dir=secret_filepath, file_name=secret_filename),
                                                creds_dir=secret_filepath)
    client = gspread_pandas.spread.Spread(
        spread='SMI Master Field Sheet', creds=credentials)

    df = clean_master_sheet()

    logging.info(f'{df.head(2)}')

    """Dictionary to replace planned crops with standardized names for SMS
            You can add more by inserting a comma then writing a new key:value pair.
            The key is before the colon and is what you are trying to replace.
            The value is after the colon and that is what you are replacing every instance of the key with.
            e.g. in the code below "CC BARLEY" gets replaced with "BARLEY" """

    print(df['FALL2019'].unique())

    # main()
