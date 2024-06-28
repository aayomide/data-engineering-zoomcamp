
# define the base docker image to build on
FROM python:3.9.1

# install pandas inside the base container and create a new image based on that
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

# set up the working directory inside the container
WORKDIR /app

# copy the script to the container
COPY ingest_data.py ingest_data.py 

# define the entrypoint and first command docker show run
# here docker first runs the ingest_data script
ENTRYPOINT [ "python", "ingest_data.py"]

