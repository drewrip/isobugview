from django.db import models

class Job(models.Model):
    key = models.CharField(max_length=100)
    log = models.TextField()
    schema = models.TextField()
    finished_file = models.FilePathField(path="/jobs")
    status = models.CharField(max_length=16)
    
