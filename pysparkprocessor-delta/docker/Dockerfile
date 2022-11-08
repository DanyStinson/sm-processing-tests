
FROM 173754725891.dkr.ecr.us-east-1.amazonaws.com/sagemaker-spark-processing:3.2-cpu-py39-v1.0

RUN pip3 install pandas delta-spark==2.0.1 tldextract spark-nlp==3.4.1 azure-cosmos==4.2.0 pycryptodome

ENV PYTHONUNBUFFERED=TRUE

ENTRYPOINT ["python3"]
