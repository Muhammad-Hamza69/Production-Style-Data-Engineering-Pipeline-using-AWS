# Rebuild trigger: AWS environment recreated from scratch (2026-07-17).
# Rebuild trigger: AWS environment recreated from scratch (2026-07-16).

FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY lambda_function.py ${LAMBDA_TASK_ROOT}


CMD ["lambda_function.lambda_handler"]

