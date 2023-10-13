FROM python

RUN apt-get update && \
    apt-get -y install pip
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY main.py .

ENTRYPOINT ["python3", "main.py"]
