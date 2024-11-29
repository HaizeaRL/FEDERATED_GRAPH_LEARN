
import subprocess
import os

# create corresponding paths
root_path = "/usr/local/app/"
tmp_path = os.path.join(root_path, "tmp")
src_path = os.path.join(root_path, "src")

# read role file
role_file = open(os.path.join(tmp_path,"role.txt"), "r") 
role = role_file.read().strip()
role_file.close() 

print("Distpatcher:: ROLE:", role)

# dispatch tasks according to role
if role and "SERVER" in role:
    subprocess.call(["python", os.path.join(src_path, "server.py")])
else:
    subprocess.call(["python", os.path.join(src_path, "client.py")])