import arcgis
from arcgis import GIS

username, password = "consultor_oficial_project_esri_co", "Es1vr7njWh34"

gis = GIS("https://project-esri-co.maps.arcgis.com", username, password)

feature_layer_item = gis.content.search("0065d19f754c4fc6ab2d3c8bf04f7678")[0]
flayers = feature_layer_item.layers
flayer = flayers[0]

print(flayers)