FROM python
ADD . .
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
CMD ["python", "postgresql.py", "testdb.py"]