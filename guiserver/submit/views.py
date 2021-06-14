from django.shortcuts import render, redirect
from submit.models import Job

import hashlib, time, subprocess

import os.path
from os import path

def salthash(input):
    return hashlib.sha256((input+str(time.time())).encode('utf-8')).hexdigest()[:16]
    
def submit(request):
    return render(request, 'submit.html', {})

def get_all_jobs(request):
    jobs = Job.objects.all()
    for i in range(len(jobs)):
        if path.exists(jobs[i].finished_file):
            jobs[i].status = "DONE"
        else:
            jobs[i].status = "WORKING"

    context = {
        'jobs': jobs
    }

    return render(request, 'all_jobs.html', context)

def get_job(request, key):
    job = Job.objects.get(key=key)

    # Don't just check for existence
    if path.exists(job.finished_file):
        job.status = "DONE"
        f = open(job.finished_file, "r")
        job.result = f.read()
        f.close()
    else:
        job.status = "WORKING"
            
    context = {
        'job': job
    }

    return render(request, 'status.html', context)

def create_job(request):
    if request.method == "POST":

        raw_schema = request.FILES["sql_schema"].file.read().decode("utf-8")
        raw_log = request.FILES["sql_log"].file.read().decode("utf-8")
                
        logHash = salthash(raw_log)
        
        newJob = Job(key=logHash, finished_file="jobs/"+logHash+"/report.log", log=raw_log, schema=raw_schema)
        
        newJob.save()

        os.mkdir("jobs/"+logHash)

        with open("jobs/"+logHash+"/schema.csv", "w+") as f:
            f.write(raw_schema)

        with open("jobs/"+logHash+"/sql.log", "w+") as f:
            f.write(raw_log)

        
        subprocess.Popen(["./runisodiff.sh", logHash])

    return redirect('/status/'+logHash)
