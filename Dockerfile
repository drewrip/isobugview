FROM ubuntu:latest
ENV PYTHONUNBUFFERED 1
COPY /guiserver .
COPY /db-isolation .
RUN apt-get -y update
RUN apt-get -y install python pip
RUN pip3 install django pyyaml pglast
EXPOSE 8000
CMD python3 manage.py runserver 0.0.0.0:8000