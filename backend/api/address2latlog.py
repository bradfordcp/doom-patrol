from geopy.geocoders import Nominatim

class geofind():
    def __init__(self):
        self.geolocator = Nominatim(user_agent="disaster_alert")

    def lookuplatlong(self, address):
        location = self.geolocator.geocode(address)
        return {"latitude":location.latitude, "longitude":location.longitude}

    def lookupaddress(self, latlong):
        pass
