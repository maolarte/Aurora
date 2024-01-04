from modules.custom_geo_functions import getDistance


data = {
    "p1": [1, 1],
    "p2": [4, 6],
}

if __name__ == "__main__":
    distance = getDistance(start=data['p1'], end=data["p2"])
    print(distance)
