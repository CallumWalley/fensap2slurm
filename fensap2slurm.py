#!/usr/bin/env python

import re, os

oldsolvercmd = os.environ['PWD'] + "/.solvercmd"

# Open file
step_type_defaults = {
    "fensap": {
        "this_step": "fensap",
        "copy":"roughness.dat.ice.${SHOT_PADDED_LAST} roughness.dat",
        "remove":"fensapstop.txt.fensap.${SHOT_PADDED}",
        "executable": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/fensapMPI",
        "waitfile": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/testWaitFileNormally",
        "inputs":"-f files.fensap.${SHOT_PADDED} -s fensapstop.txt.fensap.${SHOT_PADDED}",
        "move":"",
        "default_time": "00:00:10",
        "default_mem_per_cpu": "1500",
        "default_nodes": "1",
        "default_ntasks_per_node":"1"                
    },
    "drop": {
        "this_step": "drop",
        "copy":"",
        "remove":"fensapstop.txt.drop.${SHOT_PADDED}",
        "executable": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/fensapMPI",
        "waitfile": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/testWaitFileNormally",
        "inputs":"-f files.drop.${SHOT_PADDED} -s fensapstop.txt.drop.${SHOT_PADDED}",
        "move":"",
        "default_time": "00:00:10",
        "default_mem_per_cpu": "1500",
        "default_nodes": "1",
        "default_ntasks_per_node":"1"
    },
    "ice": {
        "this_step": "ice",
        "copy":"",
        "remove":"",
        "executable": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin//opt/nesi/mahuika/ANSYS/v192/fensapice/bin/ice3dMPI",
        "waitfile": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/testWaitFileNormally ice3dstop.txt.ice.${SHOT_PADDED} .solvercmd.out",
        "inputs":"-f config.ice.${SHOT_PADDED} -s ice3dstop.txt.ice.${SHOT_PADDED}",
        "move":"",
        "default_time": "00:00:10",
        "default_mem_per_cpu": "1500",
        "default_nodes": "1",
        "default_ntasks_per_node":"1"
    },
    "griddisp": {
        "this_step": "griddisp",
        "copy":"",
        "remove":"",
        "executable": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/fensapMPI",
        "waitfile": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/testWaitFileNormally",
        "inputs":"-f files.griddisp.${SHOT_PADDED} -s fensapstop.txt.griddisp.${SHOT_PADDED}",
        "move":"\"grid.ice.${SHOT_PADDED}.disp\" \"grid.ice.${SHOT_PADDED_NEXT}",
        "default_time": "00:00:10",
        "default_mem_per_cpu": "1500",
        "default_nodes": "1",
        "default_ntasks_per_node":"1"
    },
    "other": {
        "this_step": "other",
        "copy":"",
        "remove":"",
        "executable": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/fensapMPI",
        "waitfile": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/testWaitFileNormally",
        "inputs":"-f files.fensap.${SHOT_PADDED} -s fensapstop.txt.fensap.${SHOT_PADDED}",
        "move":"",
        "default_time": "10:00:00",
        "default_mem_per_cpu": "1500",
        "default_nodes": "4",
        "default_ntasks_per_node":"16"
    },
}

default_script = """#!/bin/bash -e

#SBATCH --job-name SHOT${SHOT}_%(this_step)s
#SBATCH --time %(default_time)s
#SBATCH --mem-per-cpu %(default_mem_per_cpu)s
#SBATCH --nodes %(default_nodes)s
#SBATCH --ntasks-per-node %(default_ntasks_per_node)s
#SBATCH --hint nomultithread
#SBATCH --open-mode     append # This sets SLURM to append, rather than overwrite output.
#SBATCH --output    .solvercmd.out   # Log
#SBATCH --licenses ansys_r@uoa_foe:1,ansys_hpc@uoa_foe:16 # HPC licences should be equal to number CPUs minus 16

#============================================#
# Current shot: ${SHOT}
# %(progress)s
#============================================#

env

echo "Starting shot ${SHOT}, %(this_step)s (step $STEP)"
echo STEP:${SHOT}_fensap | tee -a .solvercmd.out

if [ "${SHOT}" == "$(cat .restart)" ]; then echo "WARNING: STEP in '.restart' does not match STEP from current run."; fi

rm -f fensapstop.txt.fensap.$(printf %%06d $SHOT)
#srun %(executable)s  2>&1 | tee -a .solvercmd.out
#%(waitfile)s

echo "Job completed successfully"

# Save Current step
echo $STEP > .restart

# Iterate Shot
%(shot_iterator)s
echo "Next shot, shot $SHOT"


# Iterate Step
export STEP=$((STEP+1))
echo "Next step, step $STEP"

if [ "$SHOT" -le "%(end_shot)s" ]; then
    
    envsubst < template_%(next_step)s.sl > .shot${SHOT}_%(next_step)s.sl
    echo "Creating new slurm script '.shot${SHOT}_%(next_step)s.sl' from template 'template_%(next_step)s.sl'"

    #rm .shot${SHOT}_%(this_step)s.sl
    sbatch .shot${SHOT}_%(next_step)s.sl
    echo "Submitted next stage %(next_step)s, shot $SHOT"

else
    echo "Workflow completed successfully"
    rm .shot*_*.sl
fi
"""

step_types = []
shots = []

# def convert_solvercmd():
with open(oldsolvercmd, "r") as file:
    # Read next line
    solvercmd_txt = file.read()

print(solvercmd_txt)

master_pattern = re.compile(r"echo STEP:(?P<shot>\d*)_(?P<type>\w*)", flags=re.M | re.DEBUG)
matches = master_pattern.finditer(solvercmd_txt)

# This is the step at which the simulation will be considered finished
end_step = 0

for match in matches:
    match_dict = match.groupdict()
    if not match_dict["type"] in step_types:
        step_types.append(match_dict["type"])
    if not match_dict["shot"] in shots:
        shots.append(match_dict["shot"])

    end_step+=1

print(("Workflow will run " + " => ".join(step_types ) + " " + str(len(shots)) + " times"))

for i in range(len(step_types)):

    this_step=step_types[i]
    sub_values=step_type_defaults[this_step]

    if i >= len(step_types)-1:
        sub_values["next_step"]=step_types[0]
        sub_values["shot_iterator"]="export SHOT=$((SHOT+1))\nexport SHOT_PADDED_LAST=$SHOT_PADDED\nexport SHOT_PADDED=$SHOT_PADDED_NEXT\nexport SHOT_PADDED_NEXT=$(printf %06d$(($SHOT+1)))"
    else:
        sub_values["next_step"]=step_types[i+1]
        sub_values["shot_iterator"]="# Next step will use same shot"
    
    sub_values["end_shot"]=shots[-1]

    # Print summary of steps
    _progress="==> "
    ii=0
    while ii < i:
        _progress+=step_types[ii]+" ==> "
        ii+=1
    _progress+="[[" + step_types[i] + "]]"
    ii=i+1
    while ii < len(step_types):
        _progress+=" ==> "  + step_types[ii]
        ii+=1

    sub_values["progress"]=_progress


    slurm_filename = "template_" + this_step + ".sl"
    print("Writing template slurm file " + "'" + slurm_filename + "'.")
    f = open(slurm_filename, "w+")
#   f.write(default_script % {**step_type_defaults[match_dict["type"]] , "next_step":next_step, "this_step":this_step})

    f.write( default_script % sub_values )
    f.close()

starting_shot="1"

solvercmd_defaults = {
    "first_step":step_types[0],
    "first_shot":"1"
}

solvercmd="""
#!/bin/bash

if [ -f .restart ]; then
  STEP=`cat .restart`
  echo "Continuing from step $STEP. this can be modified in '.restart"
else
  STEP=0
fi

export STEP
export SHOT="1"
export SHOT_PADDED="000001"
export SHOT_PADDED_NEXT="000002"
export SHOT_PADDED_LAST=""

envsubst < template_%(first_step)s.sl > .shot%(first_shot)s_%(first_step)s.sl
#cat template_%(first_step)s.sl > .shot%(first_shot)s_%(first_step)s.sl
"""

f = open("test_solvercmd", "w+")
f.write( solvercmd % solvercmd_defaults )
f.close()

#print("run:  'envsubst < " + step_types[0] + ".sl > .shot" + starting_shot + "_ " + step_types[0] + ".sl.tmp")
# solvercmd=

# if [ -f .restart ]; then
#   STEP=`cat .restart`
# else
#   STEP=0
# fi

# "000001"


# print(step_types)
# print(shots)

# for step_type in step_types:
#     time = raw_input("Expected runtime?[]: ") or default

# print(matches.groupdict())
# matches.groupdict()
# for match in master_pattern.:
#   print()

# --(?P<key>\S*)\s(?P<value>\S*)