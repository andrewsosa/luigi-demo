FROM python:3.7
EXPOSE 8082
WORKDIR /luigi-demo
RUN pip install luigi sqlalchemy
ADD luigi.cfg .
CMD luigid
