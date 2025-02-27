# Start met een python image
# haal de image met python versie 3.8 van de docker registry
FROM python:3.8-slim

# kopieer alles van de huidige folder naar de working directory in de container
# kopieert alles dus ook data.txt
COPY . . 

CMD ["python", "app.py"]