FROM python:3.10.10-slim

WORKDIR /opt/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./mapper.py" ]
