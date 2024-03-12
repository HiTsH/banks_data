
FROM python:3.9

COPY . .

WORKDIR /banks_project

COPY requirements.txt   .

RUN pip3 install -r requirements.txt

CMD ["python", "banks_project.py"]