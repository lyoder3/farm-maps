import geopandas
import re
import os

class Boundary:
    '''Class to use for sms boundary shapefiles'''
    REGEX = re.compile(r"([HJS][OCR][A-Z\W]+)(?:_)([CP]\d{2})(?=_\d{4}|_NO Year)")

    #this method creates the object, you must always pass a filename as a paremeter
    def __init__(self, filename, *args, **kwargs):
        self.filename = filename
    #these methods update the way that these objects are represented when printing to the console
    def __str__(self):
        return f"{self.tag} Boundary File"

    def __repr__(self):
        return self.filename
    
    #This allows you to access the farm name for the object as an attribute
    @property
    def farm(self):
        return Boundary.REGEX.search(self.filename).group(1)
    #Field name attribute
    @property
    def field(self):
        return Boundary.REGEX.search(self.filename).group(2)
    #combination of farm and field with underscore used to detect matches with csv files
    @property
    def tag(self):
        return self.farm + "_" + self.field
    #this method creates boundary objects for all the files in 'folder' and adds them to a dictionary
    #the values are the boundary objects in a list and the keys are the objecct's tag.
    @staticmethod
    def get_all_boundaries(folder):
        bound_dict = {}
        for f in os.listdir(folder):
            if f.endswith(".shp") and Boundary.REGEX.search(f):
                b = Boundary(filename=os.path.join(folder, f))
                bound_dict.setdefault(b.tag, []).append(b)
        return bound_dict
    #This creates a geodataframe for the file associated with the boundary object
    def gdf(self):
        gdf = geopandas.io.file.read_file(self.filename)
        gdf["tmp"] = 1
        return gdf
