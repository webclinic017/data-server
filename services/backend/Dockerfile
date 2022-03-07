FROM python:3.8

USER root
RUN pip install fastapi uvicorn

EXPOSE 80

COPY ./services/data-backend/app /app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80", "--ssl-keyfile", "/certificates/privkey.pem", "--ssl-certfile", "/certificates/fullchain.pem", "--lifespan", "on"]