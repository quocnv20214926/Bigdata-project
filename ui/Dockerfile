FROM python:3.13-slim
WORKDIR /app
COPY ui/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY ui app
EXPOSE 8501
CMD ["streamlit", "run", "app/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
