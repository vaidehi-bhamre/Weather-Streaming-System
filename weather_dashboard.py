import streamlit as st
import json
from kafka import KafkaConsumer
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import time

# Page configuration
st.set_page_config(page_title="Weather Monitor", layout="wide", page_icon="🌤️")

# Initialize Kafka Consumer
@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        'weather-data',
        bootstrap_servers=['localhost:9092'],  # Change if needed
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        consumer_timeout_ms=1000
    )

# Store latest weather data
if 'weather_data' not in st.session_state:
    st.session_state.weather_data = {}

st.title("🌤️ Real-Time Weather Monitoring Dashboard")
st.markdown("---")

consumer = get_consumer()

# Layout containers
metrics_container = st.container()
chart_container = st.container()
map_container = st.container()
table_container = st.container()
alerts_container = st.container()  # For live alerts

# Auto-refresh loop
while True:
    messages = consumer.poll(timeout_ms=1000)
    
    for topic_partition, msgs in messages.items():
        for message in msgs:
            data = message.value
            city = data.get('city')
            st.session_state.weather_data[city] = data  # Update latest data
    
    # Display data if available
    if st.session_state.weather_data:
        
        with alerts_container:
            st.subheader("⚠️ Live Alerts")
            for city, data in st.session_state.weather_data.items():
                temp = data.get('temperature', 0)
                weather = data.get('weather', 'N/A')

                # Temperature alerts
                if temp > 40:
                    st.warning(f"⚠️ High temperature alert in {city}: {temp:.1f}°C")
                elif temp < 5:
                    st.warning(f"❄️ Low temperature alert in {city}: {temp:.1f}°C")
                
                # Rain alert
                if 'rain' in weather.lower():
                    st.info(f"🌧️ Rain alert in {city}: {weather}")
        
        # Metrics
        with metrics_container:
            st.subheader("🌡️ Current Weather Metrics")
            cols = st.columns(len(st.session_state.weather_data))
            for idx, (city, data) in enumerate(st.session_state.weather_data.items()):
                with cols[idx]:
                    temp = data.get('temperature', 0)
                    humidity = data.get('humidity', 0)
                    weather = data.get('weather', 'N/A')
                    
                    st.metric(
                        label=f"{city} {weather}",
                        value=f"{temp:.1f}°C",
                        delta=f"{humidity}% humidity"
                    )
        
        # Temperature chart
        with chart_container:
            st.subheader("📈 Temperature Comparison")
            cities = list(st.session_state.weather_data.keys())
            temps = [st.session_state.weather_data[city]['temperature'] for city in cities]
            feels = [st.session_state.weather_data[city]['feels_like'] for city in cities]
            
            fig = go.Figure(data=[
                go.Bar(name='Actual Temp', x=cities, y=temps, marker_color='rgb(55, 83, 109)'),
                go.Bar(name='Feels Like', x=cities, y=feels, marker_color='rgb(26, 118, 255)')
            ])
            fig.update_layout(barmode='group', height=400, yaxis_title="Temperature (°C)")
            st.plotly_chart(fig, use_container_width=True)
        
        # Humidity and wind charts
        with map_container:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("💧 Humidity Levels")
                humidity = [st.session_state.weather_data[city]['humidity'] for city in cities]
                fig_humidity = px.bar(
                    x=cities,
                    y=humidity,
                    color=humidity,
                    color_continuous_scale='Blues',
                    labels={'x': 'City', 'y': 'Humidity (%)'}
                )
                fig_humidity.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig_humidity, use_container_width=True)
            
            with col2:
                st.subheader("💨 Wind Speed")
                wind = [st.session_state.weather_data[city]['wind_speed'] for city in cities]
                fig_wind = px.bar(
                    x=cities,
                    y=wind,
                    color=wind,
                    color_continuous_scale='Greens',
                    labels={'x': 'City', 'y': 'Wind Speed (km/h)'}
                )
                fig_wind.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig_wind, use_container_width=True)
        
        # Detailed table
        with table_container:
            st.subheader("📋 Detailed Weather Data")
            df_data = []
            for city, data in st.session_state.weather_data.items():
                df_data.append({
                    'City': city,
                    'Temperature': f"{data['temperature']:.1f}°C",
                    'Feels Like': f"{data['feels_like']:.1f}°C",
                    'Weather': data['weather'],
                    'Humidity': f"{data['humidity']}%",
                    'Pressure': f"{data['pressure']} hPa",
                    'Wind Speed': f"{data['wind_speed']:.1f} km/h"
                })
            df = pd.DataFrame(df_data)
            st.dataframe(df, use_container_width=True, hide_index=True)
    
    else:
        st.info("⏳ Waiting for weather data... Make sure the producer is running!")
    
    # Wait a few seconds before next refresh
    time.sleep(5)
    st.rerun()
