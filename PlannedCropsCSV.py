import re
import pandas as pd
import os
import numpy as np
class PlannedCropsCSV:
    REGEX = re.compile(
        r"([HJS][OCR][A-Z\W]+)(?:_)([CP]\d{2})(?:_)(\d{4}SPRING|\d{4}FALL)"
    )

    def __init__(self, filename, *args, **kwargs):
        self.filename = os.path.abspath(filename)

    def __str__(self):
        return f"{self.tag} CSV File"

    def __repr__(self):
        return self.filename

    @property
    def farm(self):
        return PlannedCropsCSV.REGEX.search(self.filename).group(1)

    @property
    def field(self):
        return PlannedCropsCSV.REGEX.search(self.filename).group(2)

    @property
    def tag(self):
        return self.farm + "_" + self.field

    @property
    def season(self):
        return PlannedCropsCSV.REGEX.search(self.filename).group(3)

    def df(self):
        df = pd.read_csv(self.filename)
        df["tmp"] = 1
        return df

    @staticmethod
    def get_all_csvs(folder):
        csv_dict = {}
        for f in os.listdir(folder):
            if f.endswith(".csv") and PlannedCropsCSV.REGEX.search(f):
                cplan = PlannedCropsCSV(filename=f)
                csv_dict.setdefault(cplan.tag, []).append(cplan)
        return csv_dict

    @staticmethod
    def create_folders(
        csv_import_size, working_dir="F:\\Farm\\FarmDataAutomation\\CropPlans"
    ):
        folders = int(np.ceil(csv_import_size / 1000))
        n = int(5 * np.ceil(np.ceil((csv_import_size / folders) / 5)))
        for i in range(1, folders + 1):
            os.makedirs(os.path.join(working_dir, f"CropPlans {i}"), exist_ok=True)
        return folders, n

