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
COPY api.py /app/

# Expose port
EXPOSE 6000

# run FastAPI
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "6000"]
