from django.shortcuts import render, redirect
from submit.models import Job

import hashlib, time

def salthash(input):
    return hashlib.sha256((input+str(time.time())).encode('utf-8')).hexdigest()[:16]
    
def submit(request):
    return render(request, 'submit.html', {})

def get_all_jobs(request):
    jobs = Job.objects.all()
    context = {
        'jobs': jobs
    }

    return render(request, 'all_jobs.html', context)

def get_job(request, key):
    job = Job.objects.get(key=key)
    context = {
        'job': job
    }

    return render(request, 'status.html', context)

def create_job(request):
    if request.method == "POST":
        raw_log = request.POST["sql_log"]
        logHash = salthash(raw_log)
        newJob = Job(key=logHash, finished_file="jobs/"+logHash+".txt", log=raw_log)
        newJob.save()

    return redirect('/status/'+logHash)
