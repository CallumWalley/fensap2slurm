#!/usr/bin/env python

import re, os, sys, shutil

input_grid_match=""
matches=""
step_types=[]
shots=[]
path_solvercmd_root=""

def _input():
    global path_solvercmd_root 
    def _copy():
        shutil.copyfile(path_solvercmd_root + "/.solvercmd", path_solvercmd_root+"/.old.solvercmd")
        print("Renamed FENSAP generated '.solvercmd' to '.old.solvercmd'")

    def _parse(path_solvercmd):
        """Try reading .solvercmd"""

        with open(path_solvercmd, "r") as file:
            shebang = file.readline().strip()

            if shebang == "#!/bin/bash -e":
                raise Exception("'Already SLURMified.")
            elif shebang != "#!/bin/sh":
                raise Exception("Does not look like a valid .solvercmd file.")
            
            solvercmd_txt = file.read()
                    
        #master_pattern = re.compile(r"echo STEP:(?P<shot>\d*)_(?P<type>\w*)", flags=re.M)
        master_pattern = re.compile(r"(echo STEP:(?P<shot>\d*)_(?P<type>\w*))|(echo STEP:(?P<all>[^\|]*) \|)", flags=re.M)

        input_grid_pattern = re.compile(r"  mv (\"\S*\")", flags=re.M)
        
        matches = master_pattern.finditer(solvercmd_txt)

        try:
            input_grid_match = input_grid_pattern.search(solvercmd_txt).group(1)
        except:
            input_grid_match="../*.grid"


        # This is the step at which the simulation will be considered finished
        end_step = 0

        for match in matches:
            match_dict = match.groupdict()
            
            if (match_dict["type"] is not None) and (not match_dict["type"] in step_types):
                    step_types.append(match_dict["type"])
            if (match_dict["shot"] is not None) and (not match_dict["shot"] in shots):
                    shots.append(match_dict["shot"])
            if (match_dict["all"] is not None) and (not match_dict["all"].replace(" ", "_").lower() in step_types):
                    step_types.append(match_dict["all"].replace(" ", "_").lower())

            end_step+=1

        print(step_types)
        print(shots)

        if end_step < 1:
            raise Exception("No stages found")

        print(("\nWorkflow consists of " + " => ".join(step_types ) + ", " + str(len(shots)) + " shots\n"))

    if len(sys.argv) > 1:
        path_solvercmd_root=sys.argv[1].strip()
        # Strip any / from end of path.
        while path_solvercmd_root[-1]=="/": 
            path_solvercmd_root=path_solvercmd_root[0:-1]    
        if path_solvercmd_root[-10:]==".solvercmd":
            path_solvercmd_root=path_solvercmd_root[0:-10]
        try:
            print("Attempting to parse '" + path_solvercmd_root + "/.solvercmd'.")
            _parse(path_solvercmd_root+"/.solvercmd")
        except Exception as reason:
            print("Could not load '" + path_solvercmd_root + "/.solvercmd': " + str(reason))
            try:
                print("Attempting to parse '" + path_solvercmd_root + "/.old.solvercmd'.")
                _parse(path_solvercmd_root + "/.old.solvercmd")
            except Exception as reason:
                print("Could not load '" + path_solvercmd_root + "/.old.solvercmd': " + str(reason))
            else:
                return         
        else:
            return 
            _copy()


    path_solvercmd_root=os.path.abspath(os.environ['PWD'])
    
    try:
        print("Attempting to parse '" + path_solvercmd_root + "/.solvercmd'.")
        _parse(path_solvercmd_root+"/.solvercmd")
    except Exception as reason:
        print("Could not load '" + path_solvercmd_root + "/.solvercmd': " + str(reason))
        try:
            print("Attempting to parse '" + path_solvercmd_root + "/.old.solvercmd'.")
            _parse(path_solvercmd_root + "/.old.solvercmd")
        except Exception as reason:
            print("Could not load '" + path_solvercmd_root + "/.old.solvercmd': " + str(reason))
            print("Error: Could not find parsable '.solvercmd'. Check current directory or directory provided as argument is valid.")
            exit(1)
        else:
            return 
    else:
        _copy()
        return 


    

step_type_defaults = {
    "fensap": {
        "this_step": "fensap",
        "misc":"",
        "copy":"if [ \"$SHOT\" -gt \"1\" ];then cp -v  roughness.dat.ice.${SHOT_PADDED_LAST} roughness.dat;fi",
        "remove":"rm -fv fensapstop.txt.fensap.${SHOT_PADDED}",
        "executable": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/fensapMPI",
        "waitfile": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/testWaitFileNormally",
        "inputs":"-f files.fensap.${SHOT_PADDED} -s fensapstop.txt.fensap.${SHOT_PADDED}",
        "move":"",
        "default_time": "10:00:00",
        "default_mem_per_cpu": "1500",
        "default_nodes": "2",
        "default_ntasks_per_node":"16"                
    },
    "drop": {
        "this_step": "drop",
        "misc":"",
        "copy":"",
        "remove":"rm -fv fensapstop.txt.drop.${SHOT_PADDED}",
        "executable": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/fensapMPI",
        "waitfile": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/testWaitFileNormally",
        "inputs":"-f files.drop.${SHOT_PADDED} -s fensapstop.txt.drop.${SHOT_PADDED}",
        "move":"",
        "default_time": "02:00:00",
        "default_mem_per_cpu": "1500",
        "default_nodes": "1",
        "default_ntasks_per_node":"16"
    },
    "ice": {
        "this_step": "ice",
        "misc":"",
        "copy":"",
        "remove":"",
        "executable": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin//opt/nesi/mahuika/ANSYS/v192/fensapice/bin/ice3dMPI",
        "waitfile": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/testWaitFileNormally ice3dstop.txt.ice.${SHOT_PADDED}",
        "inputs":"-f config.ice.${SHOT_PADDED} -s ice3dstop.txt.ice.${SHOT_PADDED}",
        "move":"",
        "default_time": "02:00:00",
        "default_mem_per_cpu": "1500",
        "default_nodes": "1",
        "default_ntasks_per_node":"16"
    },
    "griddisp": {
        "this_step": "griddisp",
        "misc":"",
        "copy":"",
        "remove":"",
        "executable": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/fensapMPI",
        "waitfile": "/opt/nesi/mahuika/ANSYS/v192/fensapice/bin/testWaitFileNormally",
        "inputs":"-f files.griddisp.${SHOT_PADDED} -s fensapstop.txt.griddisp.${SHOT_PADDED}",
        "move":"if [ \"$SHOT\" -gt \"1\" ];then mv -v \"grid.ice.${SHOT_PADDED}.disp\" \"grid.ice.${SHOT_PADDED_NEXT}\"; else mv -v \"" + input_grid_match + "\" \"grid.ice.${SHOT_PADDED_NEXT}\";fi",
        "default_time": "02:00:00",
        "default_mem_per_cpu": "1500",
        "default_nodes": "1",
        "default_ntasks_per_node":"16"
    },
    "_custom_remeshing": {
        "this_step": "_custom_remeshing",
        "misc":"",
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
    "other": {
        "this_step": "other",
        "misc":"",
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

#SBATCH --job-name Run1_SHOT${SHOT}_%(this_step)s
#SBATCH --time %(default_time)s
#SBATCH --mem-per-cpu %(default_mem_per_cpu)s
#SBATCH --nodes %(default_nodes)s
#SBATCH --ntasks-per-node %(default_ntasks_per_node)s
#SBATCH --hint nomultithread
#SBATCH --open-mode     append # This sets SLURM to append, rather than overwrite output.
#SBATCH --output    .solvercmd.out   # Log
#SBATCH --licenses ansys_r@uoa_foe:1,ansys_hpc@uoa_foe:16 # HPC licences should be equal to number CPUs minus 16

# Set this equal to true if you want next step to submit autmatically
export CONTINUE="TRUE"

# This shouldn't be hard coded
module load ANSYS/19.2

#============================================#
%(progress)s
#============================================#

# Shouldn't need to edit anything below this point!


if [ "${SHOT}" == "$(cat .restart)" ]; then echo "WARNING: STEP in '.restart' does not match STEP from current run."; fi

# Files to be copied
%(copy)s

# Files to be removed
%(remove)s

srun %(executable)s
%(waitfile)s

# Files to be moved
%(move)s

echo "Job completed successfully"

# Iterate Shot
%(shot_iterator)s
echo "Next shot, shot $SHOT"


# Iterate Step
export STEP=$((STEP+1))

# Save Current step
echo $STEP > .restart
echo "Next step, step $STEP"

if [ "$SHOT" -le "%(end_shot)s" ]; then
    
    envsubst < template_%(next_step)s.sl > .shot${SHOT}_%(next_step)s.sl
    echo "Creating new slurm script '.shot${SHOT}_%(next_step)s.sl' from template 'template_%(next_step)s.sl'"

    #rm .shot${SHOT}_%(this_step)s.sl
    
    if [ "$CONTINUE" == "TRUE" ]; then
        sbatch .shot${SHOT}_%(next_step)s.sl
        echo "Submitted next stage %(next_step)s, shot $SHOT"
    else
        echo "'CONTINUE' not set to 'TRUE', '.shot${SHOT}_%(this_step)s.sl' must be resumed manually by running '.solvercmd'"
    fi

else
    echo "Workflow completed successfully"
    rm .shot*_*.sl
fi
"""

_input()

for i in range(len(step_types)):

    this_step=step_types[i]
    if not this_step in step_type_defaults:
        print("Step type '" + this_step + "' not recognised. Template will require modification.")
        sub_values=step_type_defaults["other"]
    else:
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

    sub_values["progress"]="# Shot ${SHOT}/" + str(len(shots)) + "\n# " + _progress

    slurm_filename = path_solvercmd_root + "/template_" + this_step + ".sl"
    print("Writing template slurm file " + "'" + slurm_filename + "'.")
    f = open(slurm_filename, "w+")
#   f.write(default_script % {**step_type_defaults[match_dict["type"]] , "next_step":next_step, "this_step":this_step})

    f.write( default_script % sub_values )
    f.close()

solvercmd_defaults = {
    "step_types":" ".join(step_types),
    "numsteps":len(step_types)
}

solvercmd="""
#!/bin/bash -e

if [ -f .restart ]; then
    export STEP=`cat .restart`
    echo "Continuing from step $STEP. this can be modified in '.restart'"
else
    echo "Writing '.restart' file, starting at step 0"
    export STEP=0
    rm -v .solvercmd.out
fi


if [ "$STEP" -gt "%(numsteps)s" ]; then
    echo "Final step reached, change '.restart' if not correct." 
    exit 1
fi

# These are the stages present in this workflow.
STEP_TYPES=( %(step_types)s )

# Based on .restart number
export SHOT=$(((STEP/%(numsteps)s)+1))
echo ""

STARTING_STAGE=${STEP_TYPES[STEP%%%(numsteps)s]}
echo "Starting at shot $SHOT, stage '$STARTING_STAGE'"

# Zero padded numbers for file names.
export SHOT_PADDED=$(printf %%06d$(($SHOT)))
export SHOT_PADDED_NEXT=$(printf %%06d$(($SHOT+1)))
export SHOT_PADDED_LAST=$(printf %%06d$(($SHOT-1)))

echo "Creating new slurm script '.shot${SHOT}_${STARTING_STAGE}.sl' from template 'template_${STARTING_STAGE}.sl'"
envsubst < template_$STARTING_STAGE.sl > .shot${SHOT}_${STARTING_STAGE}.sl

sbatch .shot${SHOT}_${STARTING_STAGE}.sl
echo "Submitted next stage .shot${SHOT}_${STARTING_STAGE}.sl"
"""


new = open(path_solvercmd_root + "/.solvercmd", "w+")
new.write( solvercmd % solvercmd_defaults )
new.close()
os.chmod(path_solvercmd_root + "/.solvercmd", 0750)
print("New .solvercmd written to " + path_solvercmd_root + "/.solvercmd")
print("\nCheck each 'template' file for appropriate resources then run './.solvercmd'")
