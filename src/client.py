import time
import os

# create corresponding paths
root_path = "/usr/local/app/"
tmp_path = os.path.join(root_path, "tmp")

# read role file
role_file = open(os.path.join(tmp_path,"role.txt"), "r") 
role = role_file.read().strip()
role_file.close() 

# obtain id and execute code
id = None
if "_" in role:
    id = role.split("_")[1]
    if id:
        while True:
            print(f"CLIENT{id} sending...")
            time.sleep(int(id))

