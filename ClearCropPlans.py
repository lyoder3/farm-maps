import os
import concurrent.futures



def delete_cmd_prompt(directory):
    os.chdir(directory)
    os.system('del /q /s /f *.* > NUL')
    os.chdir('..')
    os.rmdir(directory)

if __name__ == "__main__":
    par_dir = 'F:\\Farm\\FarmDataAutomation\\CropPlans'
    os.chdir(par_dir)
    #This finds all folders in the directory that start with "CropPlans"
    folder_list = [f for f in os.listdir() if f.startswith('CropPlans') and os.path.isdir(f)]
    print(folder_list)
    #This deletes all the files and folders in the above list
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(delete_cmd_prompt, folder_list)
    #This removes all the csv files in the directory
    os.system('del /q /s /f *.csv > NUL')