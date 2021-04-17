from pyowm.owm import OWM
from pyowm.utils.config import get_config_from
from pyowm.utils.config import get_default_config_for_subscription_type
from pyowm.utils import timestamps, formatting
import json
from confluent_kafka import Producer
from config import conf

config_dict = get_default_config_for_subscription_type('free')
owm = OWM('1a8b1d6e34be977e469e42517727e81b', config_dict)



owm.supported_languages

config_dict = owm.configuration

version_tuple = (major, minor, patch) = owm.version

reg = owm.city_id_registry()
#list_of_tuples = munich = reg.ids_for('Munich', country='DE')                 # only one: [ (2643743,, 'London, GB') ]
#list_of_tuples = reg.ids_for('london', country='GB', matching='like')           # mehrere Einträge mit bes. string im Namen
#id_of_london_city = list_of_tuples[0][0]

list_of_locations = reg.locations_for('aindling', country='DE')
aindling = list_of_locations[0] # IDs als Liste
lat = aindling.lat   # Längengrad
lon = aindling.lon   # Breitengrad

cityname = 'Aindling'

print("city: " +str(list_of_locations))

#print(id_of_london_city)
#print (list_of_tuples[0:5])
#print(lat, lon)

mgr = owm.weather_manager()
one_call = mgr.one_call(lat, lon)


observation = mgr.weather_at_place('aindling,DE')  # the observation object is a box containing a weather object
weather = observation.weather
weather.status           # short version of status (eg. 'Rain')
time = weather.reference_time(timeformat='iso')
print("timestamp: " + str(time))





weather = mgr.weather_at_place('aindling,DE').weather
temp_dict_celsius = observation.weather.temperature('celsius')
temp_dict_celsius['temp_min']
temp_dict_celsius['temp_max']
temp_dict_celsius['feels_like']
temp_dict_celsius['temp']

print("temperature: " + str(temp_dict_celsius))

wind_dict_in_meters_per_sec = observation.weather.wind()   # Default unit: 'meters_sec'
wind_dict_in_meters_per_sec['speed']
wind_dict_in_meters_per_sec['deg']
#wind_dict_in_meters_per_sec['gust']

print("wind: " + str(wind_dict_in_meters_per_sec))

rain_dict = mgr.weather_at_place('aindling,DE').weather.rain

print("rain (in mm): " + str(rain_dict))

sunrise_unix = observation.weather.sunrise_time()  # default unit: 'unix'
sunrise_iso = observation.weather.sunrise_time(timeformat='iso')
sunrise_date = observation.weather.sunrise_time(timeformat='date')
sunrset_unix = observation.weather.sunset_time()  # default unit: 'unix'
sunrset_iso = observation.weather.sunset_time(timeformat='iso')
sunrset_date = observation.weather.sunset_time(timeformat='date')

print("sunrise: " + sunrise_iso)
print("sunset: " + sunrset_iso)

one_call.current.humidity
print("humidity in %: " + str(one_call.current.humidity))

pressure_dict = observation.weather.pressure
pressure_dict['press']
pressure_dict['sea_level']
print("pressure: " + str(pressure_dict))

summary = {"sunset": sunrset_iso, "sunrise":sunrise_iso}, temp_dict_celsius, wind_dict_in_meters_per_sec, {"rain": rain_dict}, pressure_dict, {"time": time}, {"city": cityname}

# Test data
data = json.dumps(summary)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


if __name__ == "__main__":

    # Create producer
    producer = Producer(conf)

    for msg in data.split():
        producer.produce("data_eng", msg.encode("utf-8"), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
     #callbacks to be triggered.
    producer.flush()
