# get python image
FROM python:3.14-slim 

# set working dir and copy app into image
WORKDIR /app
COPY . .

# install packages
RUN pip install -r requirements.txt

# run py file from watcher.py
CMD ["python", "watcher.py"]
