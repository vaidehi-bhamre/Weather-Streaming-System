# Weather Data Pipeline
A simple real-time weather monitoring system that collects, processes, and visualizes weather data.

# What Does This Do?
This project automatically:

Collects weather data from OpenWeatherMap API
Processes the data using Apache Kafka and Spark
Stores it in MongoDB database
Shows beautiful charts and graphs on a dashboard

# Technologies Used

Python - Main programming language
Kafka - Sends data between programs
Spark - Processes large amounts of data
MongoDB - Stores all the weather information
Grafana - Creates dashboards and charts

# What You Need

Python 3.9+
Java 11
Docker (recommended)
OpenWeatherMap API Key (free)

# Features
Collects weather data every 5 minutes
Monitors multiple cities
Calculates min/max/average temperatures
Sends alerts for extreme weather
Shows real-time graphs
