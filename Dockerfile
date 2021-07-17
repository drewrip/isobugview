FROM ubuntu:latest
ENV PYTHONUNBUFFERED 1
COPY /guiserver .
COPY /db-isolation .
RUN apt-get -y update
RUN apt-get -y install python python3-pip software-properties-common
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get -y update
RUN apt-get -y upgrade libstdc++6
RUN pip3 install django pyyaml
RUN pip3 install 'pglast==1.7' --force-reinstall
EXPOSE 8000
CMD python3 manage.py runserver 0.0.0.0:8000
