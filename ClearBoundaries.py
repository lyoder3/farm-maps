import os, shutil, timeit

boundaries = r"F:\Farm\FarmDataAutomation\Boundaries"

def delete_cmd_prompt(directory):
    os.chdir(directory)
    #passes these arguments as if typing in a command prompt
    #basically says delete all the files in the directory without warning me
    os.system('del /q /s /f *.* > NUL')
    os.chdir('..')

"""Script to clear boundaries directory"""
delete_cmd_prompt(boundaries)