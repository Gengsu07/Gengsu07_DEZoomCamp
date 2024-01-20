FROM python:3.11


WORKDIR /app
COPY . .

RUN apt-get install wget
RUN pip install --no-cache-dir -r requirements.txt
 
CMD  ["python", "ingest_data.py"]
