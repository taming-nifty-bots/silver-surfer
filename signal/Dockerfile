FROM python:3.11.8-slim

ENV TZ=Asia/Kolkata
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY strategy.py executor/executor.py signal/signal_job.py ./

CMD ["python", "signal_job.py"]
