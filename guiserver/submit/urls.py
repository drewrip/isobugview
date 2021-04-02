from django.urls import path
from . import views

urlpatterns = [
    path('', views.submit, name='submit'),
    path("all", views.get_all_jobs, name="get_all_jobs"),
    path("status/<str:key>", views.get_job, name="get_job"),
    path("createJob", views.create_job, name="create_job"),
]
