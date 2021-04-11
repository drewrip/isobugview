FROM python:3
COPY /guiserver .
RUN pip install django
EXPOSE 8000
CMD python manage.py runserver