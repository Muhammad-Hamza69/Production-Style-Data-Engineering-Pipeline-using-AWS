FROM public.ecr.aws/lambda/python:3.11 AS builder

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --target /opt/python

FROM public.ecr.aws/lambda/python:3.11

COPY --from=builder /opt/python ${LAMBDA_TASK_ROOT}
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

CMD ["lambda_function.lambda_handler"]
