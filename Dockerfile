FROM astrocrpublic.azurecr.io/runtime:3.0-1

# install pandas into a virtual environment
RUN python -m venv pandas_venv && source pandas_venv/bin/activate && \
    pip install --no-cache-dir pandas && deactivate