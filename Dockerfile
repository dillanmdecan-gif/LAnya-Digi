FROM python:3.11-slim

# Don't buffer stdout/stderr — important for Railway log streaming
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY digits_bot.py .

CMD ["python", "digits_bot.py"]
