FROM python:3.11.8-slim

ENV TZ=Asia/Kolkata
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY strategy.py ./
COPY signal/signal_job.py executor/executor.py ./

CMD ["python", "executor.py"]
