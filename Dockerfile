FROM python:3.11.8-slim

ENV TZ=Asia/Kolkata
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY strategy.py signals.py decisions.py market.py paper.py paper_state.py publisher.py signal_job.py executor.py ./

CMD ["python", "executor.py"]
