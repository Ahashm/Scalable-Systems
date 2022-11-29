FROM python:3

RUN python -m pip install requests
RUN pip install kafka-python

WORKDIR /app

COPY . .

CMD [ "python", "pythonClient.py" ]
