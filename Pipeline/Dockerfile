FROM python:3.13.11
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code 
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
RUN uv sync --locked

#RUN pip install pandas pyarrow 


COPY ingest_data.py . 

ENTRYPOINT ["python", "ingest_data.py"] 

        