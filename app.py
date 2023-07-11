import requests
from flask import Flask, jsonify, request
from ariadne import QueryType, make_executable_schema, graphql_sync, gql
from ariadne.asgi import GraphQL
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__)

type_defs = gql(open("schema.graphql").read())

query = QueryType()

cache = {}

@query.field("getWeather")
def resolve_get_weather(_, info, city, date):
    cache_key = f"{city}:{date}"
    if cache_key in cache:
        return cache[cache_key]
    
    url1 = f"https://nominatim.openstreetmap.org/search?q={city}&format=json"
    response1 = requests.get(url1)
    data1 = response1.json()

    if not data1:
        raise ValueError(f"Invalid city: {city}")

    lat = data1[0]["lat"]
    lon = data1[0]["lon"]

    url2 = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&forecast_days=7&daily=temperature_2m_max,temperature_2m_min&timezone=PST"
    response2 = requests.get(url2)
    data2 = response2.json()

    if "daily" not in data2:
        raise ValueError(f"Could not retrieve weather forecast for {city} on {date}")
    
    forecast = data2['daily']
    for n in range(7):
        if forecast["time"][n] == date:
            temp_max = forecast["temperature_2m_max"][n]
            temp_min = forecast["temperature_2m_min"][n]

            weather_data = {"city": city, "lat": lat, "lon": lon, "temperatureMax": temp_max, "temperatureMin": temp_min, "date": date}

            cache[cache_key] = weather_data
            cache_expiration = datetime.now() + timedelta(seconds=20)
            cache[cache_key]["cache_expiration"] = cache_expiration

            return weather_data
        
    raise ValueError(f"No weather data available for {city} on {date}")

schema = make_executable_schema(type_defs, query)

@app.route("/graphql", methods=["POST"])
def graphql_server():
    data = request.get_json()
    success, result = graphql_sync(schema, data, context_value=None, debug=app.debug)
    status_code = 200 if success else 400
    return jsonify(result), status_code

def clear_expired_cache():
    current_time = datetime.now()
    keys_to_delete = []

    for key in cache:
        if "cache_expiration" in cache[key] and cache[key]["cache_expiration"] < current_time:
            keys_to_delete.append(key)
    for key in keys_to_delete:
        del cache[key]


if __name__=="__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(clear_expired_cache, 'interval', minutes=1)
    scheduler.start()

    try:
        app.run()
    finally:
        scheduler.shutdown()

"""
query Query {
    getWeather(city:"Lima", date:"2023-07") {
        city
        lat
        lon
        temperatureMax
        temperatureMin
    }
}
"""