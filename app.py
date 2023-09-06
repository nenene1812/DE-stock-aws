import streamlit as st
#   with st.container():
import pandas as pd
from plotly import graph_objects as go
import plotly.express as px

from pyathena import connect



# Initialize connection 
conn = connect(aws_access_key_id='AKIA4DAZ3SRXYSZPGY6L',
               aws_secret_access_key='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',
               s3_staging_dir='s3://nasdaq-index-etl/query_results/',
               region_name='us-east-1')

# Execute query
last_update = pd.read_sql("""
        SELECT MAX(data_date) as data_date
        FROM "nasdaq"."transformed_data"
        WHERE "symbol" = 'NASDAQ:Composite'
          """, conn) 

price_metric = pd.read_sql(f"""
        SELECT "symbol",max(price) as price, max(change_percent) change_percent, 
        MAX(volume) as volume
        FROM "nasdaq"."transformed_data"
        WHERE data_date = '%s'
        GROUP BY 1 
                           """% last_update['data_date'][0], conn)

nasdaq_last30 = pd.read_sql(f"""  
            SELECT data_date,max(price) as price, max(change_percent) change_percent, 
            MAX(volume) as volume
            FROM "nasdaq"."transformed_data"
            WHERE symbol = 'NASDAQ:Composite'
            GROUP BY 1 
            ORDER BY 1 asc 

            """, conn)

vinfast_last30 = pd.read_sql(f"""  
            SELECT data_date,max(price) as price, max(change_percent) change_percent, 
            MAX(volume) as volume
            FROM "nasdaq"."transformed_data"
            WHERE symbol = 'VFS:NASDAQ'
            GROUP BY 1 
            ORDER BY 1 asc 

            """, conn)

tesla_last30 = pd.read_sql(f"""  
            SELECT data_date,max(price) as price, max(change_percent) change_percent, 
            MAX(volume) as volume
            FROM "nasdaq"."transformed_data"
            WHERE symbol = 'TSLA:NASDAQ'
            GROUP BY 1 
            ORDER BY 1 asc 

            """, conn)
# Close connection
conn.close()


# price_metric



st.set_page_config(page_title="NASDAQ INDEX DASHBOARD", page_icon="", layout="wide",  initial_sidebar_state="auto") 

st.markdown(
    """
    <style>
    .stApp {
      background-color: #0D0834;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# st.title(f"NASDAQ INDEX DASHBOARD (last updated:{last_update['data_date'][0]})")
st.markdown("""
<h1 style='text-align:center;color:#512B81;'>NASDAQ INDEX DASHBOARD</h1>
<p style='text-align:center;color:gray;font-size:20px;'>(last updated: %s)</p>
""" % last_update['data_date'][0], unsafe_allow_html=True)

# Display data
nasdaq = price_metric[price_metric['symbol'] == 'NASDAQ:Composite']
tesla = price_metric[price_metric['symbol'] == 'TSLA:NASDAQ']
vinfast = price_metric[price_metric['symbol'] == 'VFS:NASDAQ']
st.dataframe(price_metric)
metrics = [
  {"name": "NASDAQ Index", "value":  nasdaq['price'].values[0] , "change":nasdaq['change_percent'].values[0]},  
  {"name": "NASDAQ Volume", "value": nasdaq['volume'].values[0], "change": 0},
  {"name": "TESLA ", "value": tesla['price'].values[0], "change": tesla['change_percent'].values[0]},
  {"name": "VINFAST ", "value": vinfast['price'].values[0], "change": vinfast['change_percent'].values[0]}
]

col1, col2, col3, col4 = st.columns(4)

# st.markdown(f"""
# <style>
# .element-container .col-md-3 {{
#     border: 1px solid white; 
#     background-color: #8CABFF !important;
#     padding: 10px;
#     border-radius: 5px;
# }} 
# background-color: #8CABFF;
# </style>
# """, unsafe_allow_html=True)

st.markdown("""
<style>
div[data-testid="metric-container"] {
   background-color: #202449;
   border: 1px solid rgba(28, 131, 225, 0.1);
   padding: 5% 5% 5% 10%;
   border-radius: 5px;
#    color: rgb(30, 103, 119);
   overflow-wrap: break-word;
}

/* breakline for metric text         */
div[data-testid="metric-container"] > label[data-testid="stMetricLabel"] > div {
   overflow-wrap: break-word;
   white-space: break-spaces;
   color: white;
}
div[data-testid="metric-container"] > label[data-testid="stMetricValue"] > div {
			font-size: large;
}
</style>
"""
, unsafe_allow_html=True)

for i, metric in enumerate(metrics):
    col = [col1, col2, col3, col4][i]  
    name = metric["name"]
    value = metric["value"]
    change = metric["change"] 
    col.metric(name, f"{value:,.2f}", delta = f"{change:.2%}")
  


# Create sample data
data = nasdaq_last30

# Convert date column to datetime
data['Date'] = pd.to_datetime(data['data_date'])


# # Create figure 
# fig = px.line(data, x='Date', y=['price', 'volume'])

# # Customize y-axes
# fig.update_yaxes(title_text='Price', secondary_y=False,showticklabels=True) 
# fig.update_yaxes(title_text='Volume', secondary_y=True,showticklabels=True)

# # Set different y-axis ranges
# fig.update_yaxes(range=[10000, 20000], secondary_y=False)
# fig.update_yaxes(range=[1000000000, 100000000000], secondary_y=True) 

# fig.show()



# Create figure
fig = go.Figure()

# Add price line 
fig.add_trace(go.Scatter(x=data['Date'], y=data['price'],
                    name='Price'))



# Set y-axes titles
fig.update_layout(
    yaxis=dict(
        title='Price',
        titlefont=dict(
            color='#1f77b4'
        ),
        tickfont=dict(
            color='#1f77b4'
        ),
        range=['auto','auto']
    )
)


# Display figure
st.plotly_chart(fig, use_container_width=True)

fig2 = go.Figure()
# Add volume bars
fig2.add_trace(go.Bar(x=data['Date'], y=data['volume'],
                name='Volume'))

# Set y-axes titles
fig2.update_layout(
    yaxis=dict(
        title='Volume',
        titlefont=dict(
            color='#ff7f0e'
        ),
        tickfont=dict(
            color='#ff7f0e'
        ),
        # anchor='free', 
        # overlaying='y',
        # side='right',
        range=['auto','auto']
    )
)


# Display figure
st.plotly_chart(fig2, use_container_width=True)

fig2 = go.Figure()
# Add volume bars
fig2.add_trace(go.Scatter(x=data['Date'], y=data['change_percent'],
                name='Volatility'))

# Set y-axes titles
fig2.update_layout(
    yaxis=dict(
        title='Volatility',
        titlefont=dict(
            color='#ff7f0e'
        ),
        tickfont=dict(
            color='#ff7f0e'
        ),
        # anchor='free', 
        # overlaying='y',
        # side='right',
        range=['auto','auto']
    )
)


# Display figure
st.plotly_chart(fig2, use_container_width=True)


# # Create sample data
# data = pd.DataFrame({
#   'Date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05'],
#   'Value': [10, 15, 20, 25,-1.22]   
# })

# # Convert column to datetime
# data['Date'] = pd.to_datetime(data['Date'])

# # Set index to be date
# data = data.set_index('Date')

# # Default date  
# date = data.index[-1]

# # Allow date input
# date = st.date_input('Select date', value=date)

# # Filter data to selected date
# data = data.loc[:date] 

# # Calculate percentage change
# data['Change'] = data['Value'].pct_change() 

# # Create chart
# fig = px.line(data, x=data.index, y='Change', 
#               labels={'value':'Percentage Change'})

# # Display chart 
# st.plotly_chart(fig, use_container_width=True)

# Create the charts
tesla_last30['Date'] = pd.to_datetime(tesla_last30['data_date'])
vinfast_last30['Date'] = pd.to_datetime(vinfast_last30['data_date'])

fig3 = px.line(tesla_last30, x='Date', y='price', title = 'Tesla stock price ($USD)')
fig4 = px.line(vinfast_last30, x='Date', y='price',title = 'Vinfast stock price ($USD)')

# Display side-by-side
left_column, right_column = st.columns(2)
left_column.plotly_chart(fig3, use_container_width=True)
right_column.plotly_chart(fig4, use_container_width=True)


# Create the charts

fig3 = px.bar(tesla_last30, x='Date', y='volume', title = 'Tesla stock volume')
fig4 = px.bar(vinfast_last30, x='Date', y='volume',title = 'Vinfast stock volume')

# Display side-by-side
left_column, right_column = st.columns(2)
left_column.plotly_chart(fig3, use_container_width=True)
right_column.plotly_chart(fig4, use_container_width=True)

tesla_last30['Volatility'] = tesla_last30['change_percent']*100
vinfast_last30['Volatility'] = vinfast_last30['change_percent']*100


fig3 = px.line(tesla_last30, x='Date', y='Volatility', title = 'Tesla stock volatility (percent)')
fig4 = px.line(vinfast_last30, x='Date', y='Volatility',title = 'Vinfast stock volatility (percent)')

# Display side-by-side
left_column, right_column = st.columns(2)
left_column.plotly_chart(fig3, use_container_width=True)
right_column.plotly_chart(fig4, use_container_width=True)
