from dotenv import load_dotenv
import os
from os.path import dirname, abspath

env_path = os.path.join(dirname(dirname(abspath(__file__))), '.env')

load_dotenv(dotenv_path=env_path)