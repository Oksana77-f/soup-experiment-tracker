FROM node:22-alpine AS ui
WORKDIR /build
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.11-slim AS runtime
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY tracker ./tracker
COPY --from=ui /build/dist ./frontend/dist
CMD ["uvicorn", "tracker.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--timeout-graceful-shutdown", "2"]

FROM runtime AS test
COPY requirements-test.txt .
RUN pip install --no-cache-dir -r requirements-test.txt && python -m playwright install --with-deps --only-shell chromium
COPY pytest.ini .
COPY tests ./tests
COPY scripts ./scripts
CMD ["python", "-m", "pytest", "-ra"]
