import collections
import datetime
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

from MasterSheet import MasterSheet
from PlannedCropsCSV import PlannedCropsCSV
from Boundary import Boundary
import pandas as pd


#suppresses a warning that doesn't affect what we're doing
pd.options.mode.chained_assignment = None


class CropPlan:
    '''This class takes a boundary and then any number of csvs as arguments. Be sure the boundary object 
    is the first object followed by the csvs as individual arguments. '''

    def __init__(self, boundary, *args, **kwargs):
        self.boundary = boundary
        i = 1
        for attr in args:
            self.__dict__[f"csv{i}"] = attr
            i += 1
    #This method takes an output directory as argument and creates crop plan shape files for each csv in the object
    #They are put in the output directory. If you pass no directory, it will put them in the main crop plans folder
    def to_shape(self, directory="F:\\Farm\\FarmDataAutomation\\CropPlans", **kwargs):
        for attr, value in self.__dict__.items():
            if str(attr).startswith("csv"):
                f = getattr(self, attr)
                df = pd.merge(self.boundary.gdf(), f.df(), on="tmp")
                out_file = os.path.join(directory, f"Crop Plans {f.tag}_{f.season}.shp")
                df.to_file(out_file)


class MatchedTuple(tuple):
    '''Class which is exactly like a normal tuple (a list that can't be changed after creation) except the length is based on the number
    of csv objects in the tuple'''

    def __len__(self):
        return len([f for f in self if repr(f).endswith(".csv")])


class MatchedTupleList(collections.UserList):
    '''This is a speical list which calculates it's length based on the number of csv files in the matched up tuples in the list'''
    def __len__(self):
        return sum(len(f) for f in self)

#This function takes a variable number of dictionaries as arguments and combines their values based on matching keys
def combine_dicts(*args):
    result = {}
    for dic in args:
        for key in result.keys() | dic.keys():
            if key in dic:
                if type(dic[key]) is list:
                    result.setdefault(key, []).extend(dic[key])
                else:
                    result.setdefault(key, []).append(dic[key])
    return result


#This puts out a csv file called "_Check.csv" with all fields that have planned crops and no boundaries
#It also puts out fields which have bounaries but come up with no planned crops on the master sheet
def gather_problem_fields(boundaries, csvs, msheet):
    result = combine_dicts(boundaries, csvs)

    files = [v for v in result.values()]

    no_cropplans = []
    no_boundaries = []

    for i in files:
        if all(isinstance(x, PlannedCropsCSV) for x in i):
            try:
                no_boundaries.extend(i)
            except TypeError:
                no_boundaries.append(i)
        elif all(isinstance(x, Boundary) for x in i):
            try:
                no_cropplans.extend(i)
            except TypeError:
                no_boundaries.append(i)
        else:
            pass

    no_cropplans = sorted(list(set([i.tag for i in no_cropplans])))
    no_boundaries = sorted(list(set([i.tag for i in no_boundaries])))

    no_cropplans = [i.split("_") for i in no_cropplans]
    no_boundaries = [i.split("_") for i in no_boundaries]

    check_df = msheet.df[
        msheet.df.index.isin(no_cropplans) | msheet.df.index.isin(no_boundaries)
    ]

    check_df.loc[check_df.index.isin(no_cropplans), "Reason"] = "No Crop Plans"
    check_df.loc[check_df.index.isin(no_boundaries), "Reason"] = "No Boundary"

    check_df.to_csv("_Check.csv")


#This is what is ran everytime you run this script. I put it in this separate main method to tidy up the script
def main():
    working_dir = "F:\\Farm\\FarmDataAutomation\\CropPlans"
    #Sets the working directory as cropplans folder
    os.chdir(working_dir)

    '''creates a mastersheet object called msheet
    If you want to create crop plans for a different year, put year=xxxx in the parenthesis
    i.e. MasterSheet(year=2021) would do SP21 and FA2020 crop plans
    The current configuration has the current year as the default so if you pass nothing
    like it is currently setup, it will take the current year as the year and do crop plans like above for the current year'''

    msheet = MasterSheet()

    #Cleans up the master sheet and reduces the number of columns to what we need for cropplans
    msheet.clean_df()
    #Writes the csv files for each farm, field, season combination.
    msheet.write_csvs()

    #Gathers all the boundaries into a dictionary called 'bound'
    bound = Boundary.get_all_boundaries(
        "F:\\Farm\\FarmDataAutomation\\Boundaries"
    )

    #Gathers all the csvs into a dictionary called csvs
    csvs = PlannedCropsCSV.get_all_csvs(working_dir)

    #calls the problem fields method from above on the current dictionaries of csvs and boundaries
    gather_problem_fields(bound, csvs, msheet)

    '''Makes a MatchedTupleList (called files) of MatchedTuple objects from above. See above classes for what is special about these types
    of objects.'''

    files = MatchedTupleList(
        (
            MatchedTuple((*v1, *v2))
            for k1, v1 in csvs.items()
            for k2, v2 in bound.items()
            if k1 == k2
        )
    )

    '''This creates the chunk folders based on the import size and the limitations of sms.
    The variables folders and n are used later. Folders is the number of folders and n is the number
    of csv files that will be used to create each chunk'''

    #The length is calculated by looping over all the MatchedTuples in the MatchedTupleList called files
    #The sum function adds up the length of each MatchedTuple object in the overall list

    folders, n = PlannedCropsCSV.create_folders(sum(len(f) for f in files))

    #I initialize these variables here so I can append to them in the loop below
    final = []
    chunk = MatchedTupleList()

    #Loop over the MatchedTuples in files
    for a in files:
        #Checks if the length of the current chunk is less than the max chunk size
        if len(chunk) < n:
            #If yes, then it adds the current tuple to that chunk
            chunk.append(a)
            #goes back to top of loop or exits if at end of files
        else:
            #If not, it adds the last chunk to the final list
            final.append(chunk)
            #Create a new empty chunk
            chunk = MatchedTupleList()
            #add the section to this new empty chunk
            chunk.append(a)
            #goes back to top of loop
    #This adds the last chunk to the final folder
    final.append(chunk)

    #gets ready for loop below by setting number folder to 1 to start
    i = 1

    #Loops over each chunk in final and creates the cropplans for the farm field combos in that chunk
    for chunk in final:
        #makes the output directory the correct number crop plan
        directory = f"F:\\Farm\\FarmDataAutomation\\CropPlans\\CropPlans {i}"
        #hardwires the output directory into the function for this chunk
        f = lambda iterable: CropPlan(iterable[-1], *iterable[:-1]).to_shape(directory)
        with ThreadPoolExecutor() as executor:
            executor.map(f, chunk)

        i += 1


if __name__ == "__main__":
    t1 = time.time()

    main()

    t2 = time.time()

    print(f"Ran in : {datetime.timedelta(seconds=t2-t1)}")

    input("Press Enter to exit")
