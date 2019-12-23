import glob
import os

clear_dir = 'F:\\Farm\\FarmDataAutomation\\CropPlans'

def clear_boundary_files(directory):
    exts = ('*.dbf', '*.prj', '*.shp', '*.shx', '*.cfg')
    
    for e in exts:
        print(os.path.join(directory, e))

# clear_dir = 'F:\\Farm\\FarmDataAutomation\\BoundaryShapeFiles'
# for f in os.listdir(clear_dir):
#     if f.endswith(('.dbf', '.prj', '.shp', '.shx', '.csv')):
#         file = os.path.join(clear_dir, f)
#         os.remove(file)