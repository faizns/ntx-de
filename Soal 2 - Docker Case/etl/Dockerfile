# Gunakan `python:3.11.4` sebagai base image
FROM python:3.11.4


# directory in container
WORKDIR /app

# copy requirements.txt ke dalam container
COPY requirements.txt requirements.txt

# instal dependensi dari requirements.txt
RUN pip install -r requirements.txt

# copy api.py ke container
COPY . .


# run 
ENTRYPOINT ["python", "etl.py"]
