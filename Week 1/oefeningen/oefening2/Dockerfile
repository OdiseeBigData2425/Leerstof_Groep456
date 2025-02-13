# Start met een python image
# haal de image met python versie 3.8 van de docker registry
FROM python:3.8-slim

# kopieer alles van de huidige folder naar de working directory in de container
COPY . .

# installeer de nodige packages
RUN pip install requests

CMD ["python", "app.py"]