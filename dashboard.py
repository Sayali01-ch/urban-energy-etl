#!/usr/bin/env python3
"""Flask dashboard for Urban Energy ETL Pipeline visualization."""

import os
import sys
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from flask import Flask, render_template, jsonify
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.dirname(__file__))

from src.logger import setup_logger

logger = setup_logger(__name__)

app = Flask(__name__, template_folder='templates', static_folder='static')

# Load processed data
DATA_FILE = "./data/output/energy_data_processed.csv"


def load_data():
    """Load the processed energy data."""
    if not Path(DATA_FILE).exists():
        logger.error(f"Data file not found: {DATA_FILE}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(DATA_FILE)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()


def create_energy_consumption_chart(df):
    """Create energy consumption trend chart."""
    if df.empty:
        return None
    
    daily_energy = df.groupby(df['timestamp'].dt.date)['energy_consumption_kwh'].sum().reset_index()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily_energy['timestamp'],
        y=daily_energy['energy_consumption_kwh'],
        mode='lines+markers',
        name='Daily Energy Consumption',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title='Daily Energy Consumption Trend',
        xaxis_title='Date',
        yaxis_title='Energy (kWh)',
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def create_building_chart(df):
    """Create energy consumption by building chart."""
    if df.empty:
        return None
    
    building_energy = df.groupby('building_id')['energy_consumption_kwh'].mean().reset_index()
    
    fig = px.bar(
        building_energy,
        x='building_id',
        y='energy_consumption_kwh',
        title='Average Energy Consumption by Building',
        labels={'building_id': 'Building', 'energy_consumption_kwh': 'Energy (kWh)'},
        color='energy_consumption_kwh',
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(
        height=400,
        template='plotly_white',
        hovermode='x unified'
    )
    
    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def create_temperature_chart(df):
    """Create temperature trend chart."""
    if df.empty:
        return None
    
    hourly_temp = df.groupby(df['timestamp'].dt.hour)['temperature_celsius'].mean().reset_index()
    hourly_temp.columns = ['hour', 'temperature']
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hourly_temp['hour'],
        y=hourly_temp['temperature'],
        mode='lines+markers',
        name='Temperature',
        line=dict(color='#ff7f0e', width=3),
        marker=dict(size=8),
        fill='tozeroy'
    ))
    
    fig.update_layout(
        title='Average Temperature by Hour',
        xaxis_title='Hour of Day',
        yaxis_title='Temperature (°C)',
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def create_occupancy_chart(df):
    """Create occupancy distribution chart."""
    if df.empty:
        return None
    
    occupancy_dist = df['occupancy_count'].value_counts().sort_index().reset_index()
    occupancy_dist.columns = ['occupancy', 'frequency']
    
    fig = px.histogram(
        df,
        x='occupancy_count',
        nbins=30,
        title='Occupancy Count Distribution',
        labels={'occupancy_count': 'Occupancy Count', 'count': 'Frequency'},
        color_discrete_sequence=['#2ca02c']
    )
    
    fig.update_layout(
        height=400,
        template='plotly_white'
    )
    
    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def get_statistics(df):
    """Calculate statistics from the data."""
    if df.empty:
        return {
            'total_records': 0,
            'avg_energy': 0,
            'max_energy': 0,
            'avg_temp': 0,
            'num_buildings': 0
        }
    
    return {
        'total_records': len(df),
        'avg_energy': round(df['energy_consumption_kwh'].mean(), 2),
        'max_energy': round(df['energy_consumption_kwh'].max(), 2),
        'avg_temp': round(df['temperature_celsius'].mean(), 2),
        'num_buildings': df['building_id'].nunique(),
        'date_range': f"{df['timestamp'].min().date()} to {df['timestamp'].max().date()}"
    }


@app.route('/')
def index():
    """Main dashboard page."""
    df = load_data()
    
    energy_chart = create_energy_consumption_chart(df)
    building_chart = create_building_chart(df)
    temperature_chart = create_temperature_chart(df)
    occupancy_chart = create_occupancy_chart(df)
    stats = get_statistics(df)
    
    return render_template(
        'dashboard.html',
        energy_chart=energy_chart,
        building_chart=building_chart,
        temperature_chart=temperature_chart,
        occupancy_chart=occupancy_chart,
        stats=stats
    )


@app.route('/api/data')
def api_data():
    """API endpoint to get raw data."""
    df = load_data()
    
    if df.empty:
        return jsonify({'error': 'No data available'}), 404
    
    return jsonify({
        'total_rows': len(df),
        'buildings': df['building_id'].unique().tolist(),
        'date_range': {
            'start': df['timestamp'].min().isoformat(),
            'end': df['timestamp'].max().isoformat()
        }
    })


@app.route('/api/building/<building_id>')
def api_building(building_id):
    """API endpoint to get specific building data."""
    df = load_data()
    
    if df.empty:
        return jsonify({'error': 'No data available'}), 404
    
    building_data = df[df['building_id'] == building_id]
    
    if building_data.empty:
        return jsonify({'error': f'Building {building_id} not found'}), 404
    
    return jsonify({
        'building_id': building_id,
        'records': len(building_data),
        'avg_energy': round(building_data['energy_consumption_kwh'].mean(), 2),
        'max_energy': round(building_data['energy_consumption_kwh'].max(), 2),
        'avg_temp': round(building_data['temperature_celsius'].mean(), 2)
    })


if __name__ == '__main__':
    logger.info("Starting Urban Energy ETL Dashboard")
    logger.info("Dashboard available at http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)
