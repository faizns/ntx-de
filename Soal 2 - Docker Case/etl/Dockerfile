# Gunakan `python:3.11.4` sebagai base image

FROM python:3.11.4

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# directory in container
WORKDIR /app

# copy requirements.txt ke dalam container
COPY requirements.txt /app/

# instal dependensi dari requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy api.py ke container
COPY etl.py /app/


# run FastAPI
CMD ["python", "etl.py"]
