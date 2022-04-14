from django.shortcuts import render, redirect
from django.http import JsonResponse
from submit.models import Job

import hashlib, time, subprocess, json

import os.path
from os import path

import json

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
        
        newJob = Job(key=logHash, finished_file="jobs/"+logHash+"/finished.json", log=raw_log, schema=raw_schema, state="{}")
   
        newJob.save()

        os.mkdir("jobs/"+logHash)

        with open("jobs/"+logHash+"/app_db_info.csv", "w+", encoding="utf-8") as f:
            f.write(raw_schema)

        with open("jobs/"+logHash+"/app.log", "w+", encoding="utf-8") as f:
            f.write(raw_log)

        # Default parameter values
        workerThreadsP = "8"
        txnLevelCyclesK = "5"
        opLevelCyclesN = "5"
        isolationLevelI = "rc"
        searchStrategyS = "b"
        timeLimitJ = "15"
        numCycleLimitC = "25"
        randomSeedR = "123456"

        print(request.POST)

        if request.POST["worker_threads"] != "":
            workerThreadsP = request.POST["worker_threads"]

        if request.POST["transaction_cycles"] != "":
            txnLevelCyclesK = request.POST["transaction_cycles"]

        if request.POST["operation_cycles"] != "":
            opLevelCyclesN = request.POST["operation_cycles"]

        if request.POST["isolation_level"] != "":
            isolationLevelI = request.POST["isolation_level"]

        if request.POST["search_strategy"] != "":
            searchStrategyS = request.POST["search_strategy"]
            
        if request.POST["random_seed"] != "":
            randomSeedR = request.POST["random_seed"]

        if request.POST["time_limit"] != "":
            timeLimitJ = request.POST["time_limit"]

        if request.POST["num_cycle_limit"] != "":
            numCycleLimitC = request.POST["num_cycle_limit"]

        settings = {
            "workerThreadsP": workerThreadsP,
            "txnLevelCyclesK": txnLevelCyclesK,
            "opLevelCyclesN": opLevelCyclesN,
            "isolationLevelI": isolationLevelI,
            "searchStrategyS": searchStrategyS,
            "randomSeedR": randomSeedR,
            "timeLimitJ": timeLimitJ,
            "numCycleLimitC": numCycleLimitC
        }
        
        with open("jobs/"+logHash+"/settings.json", "w+", encoding="utf-8") as f:
            json.dump(settings, f, indent=4)
        
        subprocess.Popen(["./runisodiff.sh", logHash, workerThreadsP, txnLevelCyclesK, opLevelCyclesN, isolationLevelI, randomSeedR, timeLimitJ, searchStrategyS, numCycleLimitC])

    return redirect('/status/'+logHash)


def update_state(request, key):

    job = Job.objects.get(key=key)
    
    res = {}
    
    if request.method == "POST":
        job.state = request.body.decode("UTF-8")
        print("===== Set =====")
        print(json.dumps(json.loads(job.state), indent=4))
        job.save()

    return JsonResponse(res)

def get_state(request, key):

    job = Job.objects.get(key=key)

    res = ""
    
    if request.method == "GET":
        res = job.state
        print("===== Get =====")
        print(json.dumps(json.loads(job.state), indent=4))

    
    return res

def recheck(request, key):

    job = Job.objects.get(key=key)

    settings = {}
    with open("jobs/"+key+"/settings.json", "r", encoding="utf-8") as f:
        settings = json.load(f)

    if os.path.exists("jobs/"+key+"/finished.json"):
        os.remove("jobs/"+key+"/finished.json")

    jsonChanges = json.loads(request.body.decode("UTF-8"))

    conf = {}
    with open("jobs/"+key+"/conf/pglast_app.json", "r", encoding="utf-8") as f:
        conf = json.load(f)
        
    
    conf["feedbacks"] = jsonChanges["changes"]

    os.remove("jobs/"+key+"/conf/pglast_app.json")

    with open("jobs/"+key+"/conf/pglast_app.json", "w+", encoding="utf-8") as f:
        json.dump(conf, f, indent=4)
    
    subprocess.Popen(["./runrecheck.sh", key, settings["workerThreadsP"], settings["txnLevelCyclesK"], settings["opLevelCyclesN"], settings["isolationLevelI"], settings["randomSeedR"], settings["timeLimitJ"], settings["searchStrategyS"], settings["numCycleLimitC"]])
    return redirect('/status/'+key)
