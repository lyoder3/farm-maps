import os

par_dir = 'F:\\Farm\\FarmDataAutomation\\CropPlans'

folder_list = [os.path.join(par_dir, f'Crop Plans {i}') for i in range(1, 5)]


def delete_cmd_prompt(directory):
    os.chdir(directory)
    os.system('del /q /s /f *.* > NUL')
    os.chdir('..')
    os.rmdir(directory)


for folder in folder_list:
    delete_cmd_prompt(directory=folder)

os.system('del /q /s /f *.csv > NUL')