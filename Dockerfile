FROM python

RUN apt-get update && \
    apt-get -y install pip
COPY requirements.txt main.py .
RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "main.py"]
