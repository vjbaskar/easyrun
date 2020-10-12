#!/usr/bin/env python

""" easyrun
Usage:
     easyrun -h|--help
     easyrun file --jobname=<job-name> [ --partition=<p> ] [ --account=<A> ] [ --time=<D-HH:MM:SS> ] \
[--memory=<MB>] [--nodes=<N>] [--ntasks=<n>] [--log=<logfile>] [--email=<email>] COMMANDFILE
     easyrun command --jobname=<job-name> [ --partition=<p> ] [ --account=<A> ] [ --time=<D-HH:MM:SS> ] \
[--memory=<MB>] [--nodes=<N>] [--ntasks=<n>] [--log=<logfile>] [--email=<email>] COMMAND

Options:
    -h --help                   Help
    --partition=<p>             Partition to run on. eg. clincloud, clincloud-himem, clincloud-express, skylake [default: clincloud]
    --account=<a>               Account to use. eg. gottgens-ccld-sl2-cpu, gottgens-ccld-sl3-cpu [default: gottgens-ccld-sl2-cpu]
    --email=<emailid>           Email id to use. eg. vs401 [default: vs401]
    --time=<time>               Upper time limit for the job D-HH:MM:SS [default: 12:00:00]
    --memory=<mem>              Memory in MB. If -1 then it will take the default value in the partition [default: -1]
    --nodes=<nodes>             Number of nodes [default: 1]
    --ntasks=<ntasks>           Number of tasks (cores) [default: 1]
    --jobname=<jobname>         Name of the job. Will be appended to logs.
    --log=<log>                 log file. Both stdout and stderr are written in the same file. [default: jobname]
"""
from docopt import docopt
import time
import os
import pandas as pd
import subprocess

class Slurmjob:
    def __init__(self, docopt_dict):
       # print("Hello World")
       # print(docopt_dict)
       jobd = dict()
       for key in docopt_dict:
           k = key.replace('--', '')
           if isinstance(docopt_dict[key], str):
               docopt_dict[key] = docopt_dict[key].replace("'","")
           jobd[k] = docopt_dict[key]
       jobd['invoke_time']=time.strftime('%Y%m%d-%H%M%S-%s', time.localtime())
       jobd['creation_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime())

       self.job = jobd

    def job_details(self):
        #print(self.job)
        return(self.job)

    def start_job(self):

        print("Running a bash file")
        cmd = ['sbatch', self.job['slurm_file']]
        if self.job['file'] == True:
            if os.path.exists(self.job['COMMANDFILE']):
                #shellout = subprocess.Popen(cmd, stdout=subprocess.PIPE)
                shellout = subprocess.run(cmd, capture_output=True)
                #shellout = "output from shell"
                #print("Output" + shellout)
            else:
                print("Error: No input file present")
                exit(1)
        if self.job['command'] == True:
            shellout = subprocess.run(cmd, capture_output=True)
        self.job['shellout'] = shellout
        #if self.job['local'] == True:
            ## To do
        #    cmd = self.job['COMMAND']


    def write_job(self):
        jobd = self.job
        command_header = ["#!/bin/bash",
                         "#SBATCH -p " + jobd['partition'],
                         "#SBATCH -A " + jobd['account'],
                         "#SBATCH -N " + jobd['nodes'],
                         "#SBATCH -n " + jobd['ntasks'],
                         "#SBATCH --job-name " + jobd['jobname'],
                         "#SBATCH --output " + ".slurm/" + jobd['log'],
                         "#SBATCH --error " + ".slurm/" + jobd['log'],
                         "#SBATCH --mail-type BEGIN,END,FAIL",
                         "#SBATCH --mail-user " + jobd['email'],
                         "#SBATCH --time " + jobd['time'],
                         "#SBATCH -p " + jobd['partition']
                         ]
        if jobd['file'] == True:
            command_header.append("bash " + jobd["COMMANDFILE"])
        if jobd['command'] == True:
            command_header.append(jobd["COMMAND"])
        slurm_file = jobd['jobname'] + "-" + jobd['invoke_time'] + ".slurm"
        print (f"Creating slurm file: {slurm_file}")
        #print(command_header)
        with open(slurm_file, "w") as f:
            f.writelines("\n".join(command_header))
        jobd['slurm_file'] = slurm_file
        jobd['cwd'] = os.getcwd()
        #jobd['command_header'] = command_header
        self.job = jobd

    def _recorder(self, recordfile):
        jobd = self.job
        if os.path.exists(recordfile):
            fmod = "a"
            h = False
        else:
            fmod = "w"
            h = True
        df = pd.DataFrame.from_dict([jobd])
        df.to_csv(recordfile, header=h, index=False, mode=fmod)

    def _create_dirs(self):
        home_dir = os.getenv("HOME")
        slurmdir = ".slurm"
        local_recorddir = ".easyrun"
        main_recorddir = home_dir + "/.easyrun_main"
        if not os.path.exists(slurmdir):
            os.makedirs(slurmdir)
        if not os.path.exists(local_recorddir):
            os.makedirs(local_recorddir)
        #main_recorddir=home_dir + "/.easyrun_main"
        if not os.path.exists(main_recorddir):
            os.makedirs(main_recorddir)
        return [home_dir, slurmdir, local_recorddir, main_recorddir]


    def record_job(self):
        home_dir, slurmdir, local_recorddir, main_recorddir = self._create_dirs()
        jobd = self.job
        #recordfile=".easyrun/hist.cmds"
        self._recorder(recordfile = local_recorddir + "/hist.cmds")
        #recordfile = ".easyrun/hist.cmds"
        self._recorder(recordfile = main_recorddir+ "/hist.cmds")

    def bkup_job(self):
        jobd = self.job
        if jobd['file']:
            code_dir = ".slurm/codes/"
            if not os.path.exists(code_dir):
                os.makedirs(code_dir)
            subprocess.run(['cp', jobd['COMMANDFILE'], code_dir+jobd['slurm_file']+".code"], capture_output=True)


if __name__ == '__main__':
    jobd = dict()
    #arguments = docopt(__doc__, version='batch cmd v1.0', argv=["file", "--jobname='hello'",'outpt.txt'])
    arguments = docopt(__doc__, version='easyrun v1.0')
    j = Slurmjob(arguments)
    j.write_job()
    j.start_job()
    j.record_job()
    j.bkup_job()
    print(j.job)
