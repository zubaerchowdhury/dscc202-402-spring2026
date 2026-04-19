# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# notebooks/04_dashboard.py
#
# NOTEBOOK DASHBOARD
# ──────────────────
# Renders four interactive visualisations directly in this notebook using
# Plotly (map + charts) and Folium (full-featured station map).
# Run this AFTER the SDP pipeline has populated the gold tables.
#
# A static Lakeview (AI/BI) dashboard JSON is in dashboard/citibike_dashboard.json
# and can be imported at Dashboards → Import.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚲 Citi Bike NYC – Station Dashboard

# COMMAND ----------

# MAGIC %pip install folium plotly --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

GOLD_DB        = "workspace.default"
TARGET_STATION = "W 21 St & 6 Ave"

print(f"Dashboard for: {TARGET_STATION}")

# COMMAND ----------

import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium

from IPython.display import display as ipy_display, HTML

# ── Load gold tables ──────────────────────────────────────────────────────────
connections_df = (
    spark.table(f"{GOLD_DB}.gold_station_connections")
         .filter(f"target_station = '{TARGET_STATION}'")
         .toPandas()
)

hourly_df = (
    spark.table(f"{GOLD_DB}.gold_hourly_counts")
         .filter(f"target_station = '{TARGET_STATION}'")
         .toPandas()
)

weekly_df = (
    spark.table(f"{GOLD_DB}.gold_weekly_counts")
         .filter(f"target_station = '{TARGET_STATION}'")
         .toPandas()
)

# Pull the target station's own lat/lng from the connections table
target_lat = connections_df["t_lat"].iloc[0]
target_lng = connections_df["t_lng"].iloc[0]

print(f"  Connected stations : {len(connections_df):,}")
print(f"  Weeks of data      : {len(weekly_df):,}")
print(f"  Station coords     : ({target_lat:.5f}, {target_lng:.5f})")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 🗺️ Station Connection Map
# MAGIC
# MAGIC #### Each circle is an endpoint station.  
# MAGIC #### **Size** = total trips (departures + arrivals).  
# MAGIC #### **Color** = net flow (blue = more arrivals, red = more departures).

# COMMAND ----------

# ── Plotly scatter-map (no API key required with open-street-map) ─────────────
fig_map = px.scatter_mapbox(
    connections_df.dropna(subset=["lat", "lng"]),
    lat="lat",
    lon="lng",
    size="total_trips",
    color="net_flow",
    color_continuous_scale="RdBu",
    color_continuous_midpoint=0,
    hover_name="other_station",
    hover_data={
        "departure_count": True,
        "arrival_count":   True,
        "total_trips":     True,
        "net_flow":        True,
        "lat":             False,
        "lng":             False,
    },
    size_max=30,
    zoom=13,
    center={"lat": target_lat, "lon": target_lng},
    mapbox_style="open-street-map",
    title=f"Trips to / from  ·  {TARGET_STATION}",
    labels={"net_flow": "Net flow<br>(+ = more dep.)"},
    height=600,
)

# Pin the target station
fig_map.add_trace(
    go.Scattermapbox(
        lat=[target_lat],
        lon=[target_lng],
        mode="markers+text",
        marker=go.scattermapbox.Marker(size=18, color="gold", symbol="star"),
        text=[TARGET_STATION],
        textposition="top right",
        name="Target station",
        hoverinfo="text",
    )
)

fig_map.update_layout(margin={"r": 0, "t": 50, "l": 0, "b": 0})
fig_map.show()

# COMMAND ----------

# MAGIC %md ### 📅 Hourly Departures & Arrivals by Day of Week

# COMMAND ----------

# Day ordering: Mon–Sun
DOW_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

hourly_melted = hourly_df.melt(
    id_vars=["day_label", "hour"],
    value_vars=["departure_count", "arrival_count"],
    var_name="direction",
    value_name="count",
)
hourly_melted["direction"] = hourly_melted["direction"].str.replace("_count", "").str.capitalize()
hourly_melted["day_label"] = pd.Categorical(hourly_melted["day_label"], categories=DOW_ORDER, ordered=True)
hourly_melted = hourly_melted.sort_values(["day_label", "hour"])

fig_hourly = px.bar(
    hourly_melted,
    x="hour",
    y="count",
    color="direction",
    facet_col="day_label",
    facet_col_wrap=7,
    barmode="group",
    color_discrete_map={"Departure": "#EF553B", "Arrival": "#636EFA"},
    title=f"Hourly Departures & Arrivals  ·  {TARGET_STATION}",
    labels={"hour": "Hour of day", "count": "Trips", "day_label": ""},
    height=350,
)
fig_hourly.update_layout(legend_title_text="Direction")
fig_hourly.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
fig_hourly.show()

# COMMAND ----------

# MAGIC %md ### 🔥 Hourly Heatmap (Departures)

# COMMAND ----------

pivot = (
    hourly_df
    .pivot_table(index="day_label", columns="hour", values="departure_count", aggfunc="sum")
    .reindex(DOW_ORDER)
)

fig_heat = px.imshow(
    pivot,
    labels={"x": "Hour of day", "y": "Day", "color": "Departures"},
    color_continuous_scale="YlOrRd",
    aspect="auto",
    title=f"Departure Heatmap  ·  {TARGET_STATION}",
)
fig_heat.show()

# COMMAND ----------

# MAGIC %md ### 📈 Weekly Trend

# COMMAND ----------

weekly_df["week_start"] = pd.to_datetime(weekly_df["week_start"])
weekly_melted = weekly_df.melt(
    id_vars="week_start",
    value_vars=["departures", "arrivals"],
    var_name="direction",
    value_name="count",
)
weekly_melted["direction"] = weekly_melted["direction"].str.capitalize()

fig_weekly = px.line(
    weekly_melted,
    x="week_start",
    y="count",
    color="direction",
    color_discrete_map={"Departures": "#EF553B", "Arrivals": "#636EFA"},
    markers=True,
    title=f"Weekly Departures & Arrivals  ·  {TARGET_STATION}",
    labels={"week_start": "Week", "count": "Trips", "direction": ""},
    height=400,
)
fig_weekly.update_layout(hovermode="x unified")
fig_weekly.show()

# COMMAND ----------

# MAGIC %md ### 🌍 Folium Interactive Map (Rich Tooltips)

# COMMAND ----------

m = folium.Map(location=[target_lat, target_lng], zoom_start=14, tiles="CartoDB positron")

# ── Colour scale helper (blue → red by net flow) ──────────────────────────────
def net_flow_color(net_flow: float, vmin: float, vmax: float) -> str:
    if vmax == vmin:
        return "#888888"
    ratio = (net_flow - vmin) / (vmax - vmin)     # 0=pure blue, 1=pure red
    r = int(255 * ratio)
    b = int(255 * (1 - ratio))
    return f"#{r:02x}88{b:02x}"

vmin = connections_df["net_flow"].min()
vmax = connections_df["net_flow"].max()
max_trips = connections_df["total_trips"].max()

# ── Add connected stations ─────────────────────────────────────────────────────
for _, row in connections_df.dropna(subset=["lat", "lng"]).iterrows():
    radius = max(4, 25 * (row["total_trips"] / max_trips))
    color  = net_flow_color(row["net_flow"], vmin, vmax)

    folium.CircleMarker(
        location=[row["lat"], row["lng"]],
        radius=radius,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.7,
        tooltip=folium.Tooltip(
            f"<b>{row['other_station']}</b><br>"
            f"Departures: {int(row['departure_count']):,}<br>"
            f"Arrivals: {int(row['arrival_count']):,}<br>"
            f"Total: {int(row['total_trips']):,}<br>"
            f"Net flow: {int(row['net_flow']):+,}",
            sticky=True,
        ),
    ).add_to(m)

    # Line from target to this station
    folium.PolyLine(
        locations=[[target_lat, target_lng], [row["lat"], row["lng"]]],
        color=color,
        weight=max(0.5, 3 * (row["total_trips"] / max_trips)),
        opacity=0.4,
    ).add_to(m)

# ── Target station marker ──────────────────────────────────────────────────────
folium.Marker(
    location=[target_lat, target_lng],
    tooltip=folium.Tooltip(f"<b>★ {TARGET_STATION}</b>", sticky=True),
    icon=folium.Icon(color="orange", icon="star", prefix="fa"),
).add_to(m)

# Render inline
ipy_display(HTML(m._repr_html_()))
