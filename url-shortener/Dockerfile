# Use a base Python image
FROM python:3.12-slim-bullseye AS streamlit_app

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
# Copy your Streamlit application files into the container
COPY streamlit_app/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY  streamlit_app .

# Expose the port Streamlit runs on (default is 8501)
EXPOSE 8501


# Command to run the Streamlit application
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0", "--browser.gatherUsageStats", "false"]

# Use a base Python image
FROM python:3.12-slim-bullseye AS url_redirect

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY url_redirect/requirements.txt .

# install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Streamlit application files into the container
COPY url_redirect .


# Expose the port FastApi runs on (default is 6379)
EXPOSE 6379

CMD ["fastapi", "run", "main.py"]
