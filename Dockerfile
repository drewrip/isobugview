FROM python:latest
ENV PYTHONUNBUFFERED 1
COPY /guiserver .
RUN pip install django
EXPOSE 8000
CMD python manage.py runserver 0.0.0.0:8000