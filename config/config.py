import yaml
from dotenv import load_dotenv
import os

load_dotenv()

class config:

    with open('config/config.yml','r')as file:
        config_data=yaml.load(file,Loader=yaml.FullLoader)


        kafka_config = config_data['KAFKA_CONFIG']
        topics = config_data['TOPICS']












config=config()


#print(config.topics)