import os
import glob

files = glob.glob('**/*.nc', recursive=True)
for f in sorted(files):
    print(f)
    os.system("ncatted -a valid_range,CMI,mode,f,"'0,4095'" " + str(f))
