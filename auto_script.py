import os
import argparse

# Instantiate the parser
parser = argparse.ArgumentParser(description='User info for HAL')

# Required positional argument
parser.add_argument('netid', 
                    help='Users NetID')


parser.add_argument('environment_name', 
                    help='Users conda environment')

parser.add_argument('HEAD_NODE_CPUS', type=int,
                    help='users environment')

parser.add_argument('HEAD_NODE_GPUS', type=int,
                    help='users environemtn')                    

parser.add_argument('WORKER_NODE_CPUS', type=int,
                    help='users environemtn')

parser.add_argument('WORKER_NODE_GPUS', type=int,
                    help='users environemtn')


args = parser.parse_args()



# variables specific to each user (user must fill in)
user_netid = args.netid
user_environment_name = args.environment_name
HEAD_NODE_CPUS_arg = args.HEAD_NODE_CPUS
HEAD_NODE_GPUS_arg = args.HEAD_NODE_GPUS
WORKER_NODE_CPUS_arg = args.WORKER_NODE_CPUS
WORKER_NODE_GPUS_arg = args.WORKER_NODE_GPUS

# for yaml file
yaml_environment = "        - conda activate " + user_environment_name + "\n"
yaml_reservation_line = "        # - \"#SBATCH --reservation=username\" \n"
new_under_slurm_line = "      under_slurm: 0 # doesn't matter. Worker node has to under slurm \n"
yaml_head_ip_line = "      # head_ip: \"192.168.20.203\" \n"


# filename = 'text_to_copy.txt'
# file = open(filename)

user_ray_path = "RAY_PATH = \"/home/" + user_netid + "/.conda/envs/" + user_environment_name + "/lib/python3.9/site-packages/ray\"\n"
HEAD_NODE_CPUS_LINE = "HEAD_NODE_CPUS = \"" + str(HEAD_NODE_CPUS_arg) + "\" \n"
HEAD_NODE_GPUS_LINE = "HEAD_NODE_GPUS = \"" + str(HEAD_NODE_GPUS_arg) + "\" \n"
WORKER_NODE_CPUS_LINE = "WORKER_NODE_CPUS = \"" + str(WORKER_NODE_CPUS_arg) + "\" \n"
WORKER_NODE_GPUS_LINE = "WORKER_NODE_GPUS = \"" + str(WORKER_NODE_GPUS_arg) + "\" \n"


# 
# print(user_ray_path)
# print(HEAD_NODE_CPUS_LINE)
# print(HEAD_NODE_GPUS_LINE)
# print(WORKER_NODE_CPUS_LINE)
# print(WORKER_NODE_GPUS_LINE)
# 







os.system("git clone https://github.com/TingkaiLiu/Ray-SLURM-autoscaler.git")

# go into the new directory
# os.system("cd Ray-SLURM-autoscaler")

cwd_1 = os.getcwd()                 # save current directory
os.chdir(cwd_1 + '/Ray-SLURM-autoscaler')
cwd_2 = os.getcwd() 

output_file = 'deploy.py'

output_file_1 = """'''
Created by Tingkai Liu on Aug 22, 2022
'''


import subprocess
import os

''' TODO: Fill the fields below '''

# The absolute path of Ray library
""" 

output_file_2 = """# The compute node name to IP mapping
SLURM_IP_LOOKUP = \"""{
    "hal01" : "192.168.20.1",
    "hal02" : "192.168.20.2",
    "hal03" : "192.168.20.3",
    "hal04" : "192.168.20.4",
    "hal05" : "192.168.20.5",
    "hal06" : "192.168.20.6",
    "hal07" : "192.168.20.7",
    "hal08" : "192.168.20.8",
    "hal09" : "192.168.20.9",
    "hal10" : "192.168.20.10",
    "hal11" : "192.168.20.11",
    "hal12" : "192.168.20.12",
    "hal13" : "192.168.20.13",
    "hal14" : "192.168.20.14",
    "hal15" : "192.168.20.15",
    "hal16" : "192.168.20.16",
}\"""

MAX_SLURM_JOB_TIME = "01:30:00"

"""

output_file_3 = """
''' End of fields to be filled '''


if __name__ == "__main__":
    
    # Sanity check of Ray path
    while RAY_PATH.endswith('/'):
        RAY_PATH = RAY_PATH[:-1]

    RAY_SLURM_PATH = RAY_PATH + "/autoscaler/_private/slurm"
    TEMPLATE_PATH = RAY_SLURM_PATH + "/template"
    
    if not os.path.exists(RAY_PATH):
        print("Ray path is not vaild. Please fill the fields in deploy.py correctly")
        exit(0)

    #if os.path.exists(RAY_SLURM_PATH):
    #    ans = input("Ray-SLURM packages already exist. Overwrite? [y/n]: ")
    #    if ans != 'y':
    #        print("Exited")
    #        exit(0)

    os.makedirs(RAY_SLURM_PATH, exist_ok=True)
    os.makedirs(TEMPLATE_PATH, exist_ok=True)

    # Copy the files that don't need to be modified
    subprocess.run([
        "cp", 
        "slurm/empty_command_runner.py",
        "slurm/node_provider.py",
        "slurm/slurm_commands.py",
        RAY_SLURM_PATH
    ])

    subprocess.run(["cp", "slurm/template/end_head.sh", TEMPLATE_PATH])

    # Fill and copy __init__ file 
    with open("slurm/__init__.py", "r") as f:
        init = f.read()
    init = init.replace("[_DEPLOY_SLURM_IP_LOOKUP_] ", SLURM_IP_LOOKUP)
    with open(RAY_SLURM_PATH + "/__init__.py", "w") as f:
        f.write(init)

    # Fill and copy bash / Slurm templates
    with open("slurm/template/head.sh", "r") as f:
        template = f.read()
    template = template.replace("[_DEPLOY_HEAD_CPUS_]", HEAD_NODE_CPUS)
    template = template.replace("[_DEPLOY_HEAD_GPUS_]", HEAD_NODE_GPUS)
    with open(TEMPLATE_PATH + "/head.sh", "w") as f:
        f.write(template)
    
    with open("slurm/template/head.slurm", "r") as f:
        template = f.read()
    template = template.replace("[_DEPLOY_HEAD_CPUS_]", HEAD_NODE_CPUS)
    template = template.replace("[_DEPLOY_HEAD_GPUS_]", HEAD_NODE_GPUS)
    template = template.replace("[_DEPLOY_SLURM_JOB_TIME_]", MAX_SLURM_JOB_TIME)
    with open(TEMPLATE_PATH + "/head.slurm", "w") as f:
        f.write(template)
    
    with open("slurm/template/worker.slurm", "r") as f:
        template = f.read()
    template = template.replace("[_DEPLOY_WORKER_CPUS_]", WORKER_NODE_CPUS)
    template = template.replace("[_DEPLOY_WORKER_GPUS_]", WORKER_NODE_GPUS)
    template = template.replace("[_DEPLOY_SLURM_JOB_TIME_]", MAX_SLURM_JOB_TIME)
    with open(TEMPLATE_PATH + "/worker.slurm", "w") as f:
        f.write(template)

    # Fill and generate autoscaler config
    with open("slurm/example-full.yaml", "r") as f:
        template = f.read()
    template = template.replace("[_DEPLOY_RAY_PATH_]", RAY_PATH)
    template = template.replace("[_DEPLOY_RAY_TEMPLATE_PATH_]", TEMPLATE_PATH)
    template = template.replace("[_DEPLOY_HEAD_CPUS_]", HEAD_NODE_CPUS)
    template = template.replace("[_DEPLOY_HEAD_GPUS_]", HEAD_NODE_GPUS)
    template = template.replace("[_DEPLOY_WORKER_CPUS_]", WORKER_NODE_CPUS)
    template = template.replace("[_DEPLOY_WORKER_GPUS_]", WORKER_NODE_GPUS)
    with open("ray-slurm.yaml", "w") as f:
        f.write(template)

    print("Deployment completed")
"""


#print("HERE 1")

with open(output_file, "w") as output:
    
    output.write(output_file_1 + user_ray_path + output_file_2 + HEAD_NODE_CPUS_LINE + HEAD_NODE_GPUS_LINE + WORKER_NODE_CPUS_LINE + WORKER_NODE_GPUS_LINE + output_file_3)



#print("Finished with deploy.py")


os.chdir(cwd_2 + '/slurm' + '/template')

output_file = 'head.slurm'

output_file_1 = """#!/bin/bash -l

#SBATCH --cpus-per-task=[_DEPLOY_HEAD_CPUS_]
#SBATCH --gres=gpu:[_DEPLOY_HEAD_GPUS_]

#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --tasks-per-node=1
#SBATCH --time=[_DEPLOY_SLURM_JOB_TIME_]

[_PY_ADD_SLURM_CMD_]

set -x
SLURM_GPUS_PER_TASK="[_DEPLOY_HEAD_GPUS_]"

# __doc_head_address_start__

# Getting the node names
head_node="$SLURMD_NODENAME"
port="[_PY_PORT_]"
ray_client_port="[_PY_RAY_CLIENT_PORT_]"
dashboad_port="[_PY_DASHBOARD_PORT_]"
password="[_PY_REDIS_PASSWORD_]"
# __doc_head_address_end__

# __doc_head_ray_start__
head_node_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address)
ip_head=$head_node_ip:$port
echo "IP Head: $ip_head"


[_PY_INIT_COMMAND_] # To be replaced by python laucher

srun --nodes=1 --ntasks=1 -w "$head_node" \\
    ray stop
srun --nodes=1 --ntasks=1 -w "$head_node" \\
    ray start --head --node-ip-address="$head_node_ip" --port=$port --dashboard-port=$dashboad_port\\
    --num-cpus "${SLURM_CPUS_PER_TASK}" --num-gpus "${SLURM_GPUS_PER_TASK}" \\
    --ray-client-server-port "$ray_client_port" --autoscaling-config=~/ray_bootstrap_config.yaml \\
    --redis-password="$password" --block &
# __doc_head_ray_end__


sleep infinity # wait forever to presist the ray runtime

"""

with open(output_file, "w") as output:
    
    output.write(output_file_1)

#print("Finished with head.slurm")

output_file = 'worker.slurm'

output_file_1 = """#!/bin/bash -l

#SBATCH --cpus-per-task=[_DEPLOY_WORKER_CPUS_]
#SBATCH --gres=gpu:[_DEPLOY_WORKER_GPUS_]

#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --tasks-per-node=1
#SBATCH --time=[_DEPLOY_SLURM_JOB_TIME_]

[_PY_ADD_SLURM_CMD_]

set -x
SLURM_GPUS_PER_TASK="[_DEPLOY_WORKER_GPUS_]"

ip_head="[_PY_IP_HEAD_]" # To be replaced by python laucher
password="[_PY_REDIS_PASSWORD_]"

[_PY_INIT_COMMAND_] # To be replaced by python laucher

echo "Starting WORKER"
srun --nodes=1 --ntasks=1 \\
    ray start --address "$ip_head" \\
    --num-cpus "${SLURM_CPUS_PER_TASK}" --num-gpus "${SLURM_GPUS_PER_TASK}" \\
    --redis-password="$password" --block &

# __doc_worker_ray_end__

sleep infinity # wait forever to presist the ray runtime
"""

with open(output_file, "w") as output:
    
    output.write(output_file_1)

#print("Finished with worker.slurm")

# go back to original directory
os.chdir(cwd_2)

os.system("python3 deploy.py")

filename = 'ray-slurm.yaml'
file = open(filename)

content = file.readlines()      # put all the lines of the file into an array 


content[36] = new_under_slurm_line
content[42] = yaml_environment
content[44] = yaml_reservation_line

content[57] = new_under_slurm_line
content[59] = yaml_environment
content[61] = yaml_reservation_line

content[39] = yaml_head_ip_line


i = 0
with open("ray-slurm.yaml", "w") as output:

    while(i < 80):
        output.write(content[i])
        i = i + 1

os.system("ray up ray-slurm.yaml --no-config-cache --yes")








