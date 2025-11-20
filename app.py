"""
Real-Time Weather Data Dashboard
Consumes data from Kafka and stores in MongoDB
Displays real-time and historical weather analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title="Weather Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_mongodb_client(connection_string):
    """Initialize MongoDB client with connection pooling"""
    try:
        client = MongoClient(
            connection_string,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            maxPoolSize=50
        )
        # Test connection
        client.admin.command('ping')
        return client
    except (ConnectionFailure, Exception) as e:
        st.error(f"MongoDB connection failed: {e}")
        return None


def setup_sidebar():
    """Configure sidebar settings"""
    st.sidebar.title("🌤️ Weather Dashboard")
    st.sidebar.markdown("---")
    
    st.sidebar.subheader("🔌 Data Source")
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker",
        value="localhost:9092",
        help="Kafka broker address"
    )
    
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic",
        value="weather-data",
        help="Kafka topic name"
    )
    
    st.sidebar.subheader("💾 Storage")
    mongo_uri = st.sidebar.text_input(
        "MongoDB URI",
        value="mongodb+srv://virtucio_dominic:dom123@groceryinventorysystem.ir2lnmu.mongodb.net/?appName=GroceryInventorySystem",
        type="password",
        help="MongoDB connection string"
    )
    
    db_name = st.sidebar.text_input(
        "Database Name",
        value="weather_streaming",
        help="MongoDB database name"
    )
    
    collection_name = st.sidebar.text_input(
        "Collection Name",
        value="weather_data",
        help="MongoDB collection name"
    )
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "mongo_uri": mongo_uri,
        "db_name": db_name,
        "collection_name": collection_name
    }


def consume_kafka_data(config):
    """Consume real-time data from Kafka"""
    kafka_broker = config["kafka_broker"]
    kafka_topic = config["kafka_topic"]
    
    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    
    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=3000,
                max_poll_records=50
            )
        except Exception as e:
            st.error(f"Kafka connection failed: {e}")
            st.session_state[cache_key] = None
    
    consumer = st.session_state[cache_key]
    
    if consumer:
        try:
            messages = []
            msg_pack = consumer.poll(timeout_ms=2000, max_records=50)
            
            for tp, messages_batch in msg_pack.items():
                for message in messages_batch:
                    try:
                        data = message.value
                        messages.append(data)
                    except Exception as e:
                        st.warning(f"Error processing message: {e}")
            
            return messages
            
        except Exception as e:
            st.error(f"Error consuming from Kafka: {e}")
            return []
    
    return []


def store_to_mongodb(config, data_list):
    """Store consumed data to MongoDB"""
    if not data_list:
        return False
    
    try:
        client = get_mongodb_client(config["mongo_uri"])
        if not client:
            return False
        
        db = client[config["db_name"]]
        collection = db[config["collection_name"]]
        
        # Add ingestion timestamp
        for data in data_list:
            data['ingestion_time'] = datetime.utcnow()
        
        # Insert documents
        result = collection.insert_many(data_list)
        return len(result.inserted_ids)
        
    except Exception as e:
        st.error(f"MongoDB storage error: {e}")
        return False


def query_mongodb(config, time_range_hours=24, location=None):
    """Query historical data from MongoDB"""
    try:
        client = get_mongodb_client(config["mongo_uri"])
        if not client:
            return pd.DataFrame()
        
        db = client[config["db_name"]]
        collection = db[config["collection_name"]]
        
        # Build query
        time_threshold = datetime.utcnow() - timedelta(hours=time_range_hours)
        query = {"ingestion_time": {"$gte": time_threshold}}
        
        if location:
            query["location.name"] = location
        
        # Execute query
        cursor = collection.find(query).sort("ingestion_time", -1).limit(1000)
        data = list(cursor)
        
        if not data:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df_data = []
        for doc in data:
            df_data.append({
                'timestamp': doc.get('timestamp'),
                'location': doc.get('location', {}).get('name'),
                'country': doc.get('location', {}).get('country'),
                'temp_c': doc.get('current', {}).get('temp_c'),
                'temp_f': doc.get('current', {}).get('temp_f'),
                'humidity': doc.get('current', {}).get('humidity'),
                'pressure_mb': doc.get('current', {}).get('pressure_mb'),
                'wind_kph': doc.get('current', {}).get('wind_kph'),
                'condition': doc.get('current', {}).get('condition'),
                'feelslike_c': doc.get('current', {}).get('feelslike_c'),
                'cloud': doc.get('current', {}).get('cloud'),
                'visibility_km': doc.get('current', {}).get('visibility_km'),
                'uv': doc.get('current', {}).get('uv'),
                'ingestion_time': doc.get('ingestion_time')
            })
        
        df = pd.DataFrame(df_data)
        
        # Convert timestamps
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        if 'ingestion_time' in df.columns:
            df['ingestion_time'] = pd.to_datetime(df['ingestion_time'])
        
        return df
        
    except Exception as e:
        st.error(f"MongoDB query error: {e}")
        return pd.DataFrame()


def display_real_time_view(config, refresh_interval):
    """Display real-time weather data"""
    st.header("🌍 Real-Time Weather Monitoring")
    
    # Auto-refresh indicator
    refresh_state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} | Updates every {refresh_interval}s")
    
    # Consume from Kafka
    with st.spinner("🔄 Fetching latest weather data..."):
        kafka_messages = consume_kafka_data(config)
    
    if kafka_messages:
        # Store to MongoDB
        stored_count = store_to_mongodb(config, kafka_messages)
        
        if stored_count:
            st.success(f"✓ Received {len(kafka_messages)} messages | Stored {stored_count} to MongoDB")
        
        # Display latest data
        st.subheader("📡 Latest Weather Updates")
        
        cols = st.columns(min(len(kafka_messages), 3))
        
        for idx, message in enumerate(kafka_messages[:6]):
            col_idx = idx % 3
            with cols[col_idx]:
                location = message['location']['name']
                temp = message['current']['temp_c']
                condition = message['current']['condition']
                humidity = message['current']['humidity']
                wind = message['current']['wind_kph']
                
                st.markdown(f"""
                <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                            padding: 20px; border-radius: 10px; color: white; margin-bottom: 10px;'>
                    <h3 style='margin:0; color: white;'>{location}</h3>
                    <h1 style='margin:10px 0; color: white;'>{temp}°C</h1>
                    <p style='margin:5px 0;'>{condition}</p>
                    <p style='margin:5px 0;'>💧 {humidity}% | 💨 {wind} km/h</p>
                </div>
                """, unsafe_allow_html=True)
        
        # Detailed view
        with st.expander("📋 Detailed Weather Data"):
            for message in kafka_messages:
                st.json(message)
    
    else:
        st.warning("⚠️ No new messages from Kafka. Check if the producer is running.")
        st.info("💡 Start the producer with: `python producer.py --api-key YOUR_API_KEY`")


def display_historical_view(config):
    """Display historical weather analytics"""
    st.header("📊 Historical Weather Analytics")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        time_range = st.selectbox(
            "Time Range",
            options=[1, 6, 12, 24, 48, 168],
            format_func=lambda x: f"Last {x} hours" if x < 168 else "Last week",
            index=3
        )
    
    with col2:
        # Get available locations
        try:
            client = get_mongodb_client(config["mongo_uri"])
            if client:
                db = client[config["db_name"]]
                collection = db[config["collection_name"]]
                locations = collection.distinct("location.name")
                selected_location = st.selectbox("Location", ["All"] + locations)
            else:
                selected_location = "All"
        except:
            selected_location = "All"
    
    with col3:
        metric_type = st.selectbox(
            "Primary Metric",
            ["Temperature", "Humidity", "Pressure", "Wind Speed"]
        )
    
    # Query data
    with st.spinner("📥 Loading historical data..."):
        df = query_mongodb(
            config,
            time_range_hours=time_range,
            location=None if selected_location == "All" else selected_location
        )
    
    if not df.empty:
        st.success(f"✓ Loaded {len(df)} records from MongoDB")
        
        # Summary metrics
        st.subheader("📈 Current Statistics")
        metric_cols = st.columns(5)
        
        latest_data = df.iloc[0] if len(df) > 0 else None
        
        if latest_data is not None:
            with metric_cols[0]:
                st.metric("Temperature", f"{latest_data['temp_c']:.1f}°C")
            with metric_cols[1]:
                st.metric("Feels Like", f"{latest_data['feelslike_c']:.1f}°C")
            with metric_cols[2]:
                st.metric("Humidity", f"{latest_data['humidity']}%")
            with metric_cols[3]:
                st.metric("Wind Speed", f"{latest_data['wind_kph']:.1f} km/h")
            with metric_cols[4]:
                st.metric("UV Index", f"{latest_data['uv']}")
        
        # Temperature trend
        st.subheader("🌡️ Temperature Trends")
        fig_temp = px.line(
            df,
            x='ingestion_time',
            y='temp_c',
            color='location',
            title='Temperature Over Time',
            labels={'temp_c': 'Temperature (°C)', 'ingestion_time': 'Time'}
        )
        fig_temp.update_layout(height=400)
        st.plotly_chart(fig_temp, use_container_width=True)
        
        # Multi-metric comparison
        st.subheader("📊 Multi-Metric Analysis")
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Temperature', 'Humidity', 'Pressure', 'Wind Speed'),
            specs=[[{'secondary_y': False}, {'secondary_y': False}],
                   [{'secondary_y': False}, {'secondary_y': False}]]
        )
        
        for location in df['location'].unique():
            loc_data = df[df['location'] == location]
            
            fig.add_trace(
                go.Scatter(x=loc_data['ingestion_time'], y=loc_data['temp_c'],
                          name=location, legendgroup=location, showlegend=True),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=loc_data['ingestion_time'], y=loc_data['humidity'],
                          name=location, legendgroup=location, showlegend=False),
                row=1, col=2
            )
            fig.add_trace(
                go.Scatter(x=loc_data['ingestion_time'], y=loc_data['pressure_mb'],
                          name=location, legendgroup=location, showlegend=False),
                row=2, col=1
            )
            fig.add_trace(
                go.Scatter(x=loc_data['ingestion_time'], y=loc_data['wind_kph'],
                          name=location, legendgroup=location, showlegend=False),
                row=2, col=2
            )
        
        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_xaxes(title_text="Time", row=2, col=2)
        fig.update_yaxes(title_text="°C", row=1, col=1)
        fig.update_yaxes(title_text="%", row=1, col=2)
        fig.update_yaxes(title_text="hPa", row=2, col=1)
        fig.update_yaxes(title_text="km/h", row=2, col=2)
        
        fig.update_layout(height=600, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
        
        # Location comparison
        st.subheader("🗺️ Location Comparison")
        
        location_summary = df.groupby('location').agg({
            'temp_c': ['mean', 'min', 'max'],
            'humidity': 'mean',
            'wind_kph': 'mean'
        }).round(2)
        
        location_summary.columns = ['Avg Temp', 'Min Temp', 'Max Temp', 'Avg Humidity', 'Avg Wind']
        st.dataframe(location_summary, use_container_width=True)
        
        # Raw data table
        with st.expander("📄 View Raw Data"):
            st.dataframe(df.sort_values('ingestion_time', ascending=False), use_container_width=True)
    
    else:
        st.warning("⚠️ No historical data available in MongoDB")
        st.info("💡 Make sure the producer is running and data is being stored")


def main():
    """Main application"""
    st.title("🌤️ Real-Time Weather Data Dashboard")
    
    # Initialize session state
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }
    
    # Sidebar configuration
    config = setup_sidebar()
    
    # Refresh controls
    st.sidebar.markdown("---")
    st.sidebar.subheader("⚙️ Refresh Settings")
    
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh']
    )
    
    refresh_interval = 15
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=10,
            max_value=60,
            value=15,
            step=5
        )
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    st.sidebar.metric("Last Refresh", 
                     st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    
    # Main content tabs
    tab1, tab2 = st.tabs(["🌍 Real-Time Data", "📊 Historical Analytics"])
    
    with tab1:
        display_real_time_view(config, refresh_interval)
    
    with tab2:
        display_historical_view(config)


if __name__ == "__main__":
    main()
