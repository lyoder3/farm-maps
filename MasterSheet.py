import gspread_pandas
import pandas as pd
import datetime
import re
import csv

class MasterSheet(gspread_pandas.spread.Spread):
    '''Class used to read in the master sheet as a Pandas DataFrame.
    Contains some methods to clean it up and also update information on the boundary'''
    SECRET_FILEPATH = r"F:\Farm\FarmDataAutomation"
    SECRET_FILENAME = "SMS_secret.json"
    CREDENTIALS = gspread_pandas.conf.get_creds(
        config=gspread_pandas.conf.get_config(
            conf_dir=SECRET_FILEPATH, file_name=SECRET_FILENAME
        ),
        creds_dir=SECRET_FILEPATH,
    )
    SPREAD = "SMI Master Field Sheet"
    CROP_LIST = [
        "ALFALFA",
        "BARLEY",
        "CORN",
        "CORN SIL",
        "CREP",
        "DCCORN",
        "DOUBLE CROP BEANS",
        "OATS",
        "ORCHARD GRASS",
        "POTATOES",
        "RAPESEED",
        "RYE",
        "SOYBEANS",
        "TIMOTHY",
        "TRITICALE",
        "WHEAT",
        "BARLEY/CLOVER/RADISH",
        "BARELY/RADISH",
        "WHEAT/BARELY/RADISH",
        "WHEAT/TIMOTHY",
        "BARLEY/CLOVER",
        "CC RYE",
        "CC BARLEY",
        "CC OATS",
    ]

    def __init__(self, year=datetime.date.today().year, *args, **kwargs):
        self.year = year
        super(MasterSheet, self).__init__(
            spread=MasterSheet.SPREAD, creds=MasterSheet.CREDENTIALS, *args, **kwargs
        )
        self.df = self.sheet_to_df(index=0, sheet=0, header_rows=1)

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, val):
        self._df = val

    def clean_df(self):
        df = self.df
        cols = ["Farm Name", "Field", f"{self.year - 1} FALL", f"{self.year} SPRING"]
        df = df[cols]
        df = df.rename(
            columns={
                "Farm Name": "Farm",
                f"{self.year - 1} FALL": f"FALL{self.year - 1}",
                f"{self.year} SPRING": f"SPRING{self.year}",
            },
        )
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
        df = df.fillna("None")
        df = df.replace({"": "None"})
        df["GROWER"] = df.apply(
            lambda row: re.match(r"[HJS][OCRZ](?=-)", row.Farm).group(0), axis=1
        )
        df = df.set_index(["Farm", "Field"])

        self.df = df

    def write_csvs(self):
        spring_df = self.df[self.df[f"SPRING{self.year}"] != "None"]
        fall_df = self.df[self.df[f"FALL{self.year -1}"] != "None"]
        spring_df['YEAR'] = self.year
        spring_df['Season'] = 'Planting - Spring'
        fall_df['YEAR'] = self.year - 1      
        fall_df['Season'] =  'Planting - Autumn'
        for row in spring_df.itertuples():
            file_name = (
                row[0][0] + "_" + row[0][1] + "_" + f"{self.year}SPRING" + ".csv"
            )
            with open(file_name, "w+", newline="") as out:
                file_writer = csv.writer(out)
                file_writer.writerow(["FARM", "FIELD_1", "PRODUCT_1", "GROWER", "YEAR", 'Season'])
                file_writer.writerow([row[0][0], row[0][1], row[2], row[3], row[4], row[5]])
        for row in fall_df.itertuples():
            file_name = (
                row[0][0] + "_" + row[0][1] + "_" + f"{self.year - 1}FALL" + ".csv"
            )
            with open(file_name, "w+", newline="") as out:
                file_writer = csv.writer(out)
                file_writer.writerow(["FARM", "FIELD_1", "PRODUCT_1", "GROWER", "YEAR", 'Season'])
                file_writer.writerow(
                    [row[0][0], row[0][1], row[1], row[3], row[4], row[5]]
                )

    @classmethod
    def from_file(cls, file_name="SMI Master Sheet.csv"):
        pass

