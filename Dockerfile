FROM python:3.10-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/app.py .
COPY src/index.html .

EXPOSE 5000

ENV FLASK_APP=app.py

CMD ["python", "app.py"]