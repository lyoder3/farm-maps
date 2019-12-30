import os

par_dir = 'F:\\Farm\\FarmDataAutomation\\CropPlans'

folder_list = [os.path.join(par_dir, f'Crop Plans {i}') for i in range(1, 6)]

for folder in folder_list:
    try:
        os.chdir(folder)
        for x in os.listdir():
            os.remove(x)
        os.chdir('..')
        os.rmdir(folder)
    except FileNotFoundError:
        continue
os.chdir(par_dir)
for f in os.listdir():
    if f.endswith('.csv'):
        os.remove(f)