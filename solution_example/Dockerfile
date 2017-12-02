# Use a kaggle/python base image
FROM kaggle/python

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY hackathon_protocol.py /app
COPY predict_online.py /app
COPY metadata.ini /app
COPY readme.txt /app

# Define environment variables
ENV HACKATHON_CONNECT_IP "172.17.0.1"
ENV HACKATHON_CONNECT_PORT "12345"

# Run predict_online.py when the container launches
CMD ["python3", "predict_online.py"]

