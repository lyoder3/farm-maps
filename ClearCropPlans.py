import os

clear_dir = 'F:\\Farm\\FarmDataAutomation\\CropPlans'

for f in os.listdir(clear_dir):
    if f.endswith(('.dbf', '.prj', '.shp', '.shx','.csv', 'cpg')):
        file = os.path.join(clear_dir, f)
        os.remove(file)