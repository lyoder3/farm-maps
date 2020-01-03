import os, shutil, timeit

boundaries = r"F:\Farm\FarmDataAutomation\boundaries"

def delete_cmd_prompt(directory):
    os.chdir(directory)
    os.system('del /q /s /f *.* > NUL')
    os.chdir('..')

delete_cmd_prompt(boundaries)