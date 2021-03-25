import json
from tap_pipedrive import main

if __name__ == "__main__":

    main(json.load(open("config.json")))
