import requests
import tts.carter as carter
import time

while True:
    response = requests.get('https://5000-a95dadf6-90e1-4df8-9b8f-b142c6fe3607.ws-us02.gitpod.io/api/spoof_get_events/')
    print(response.text)
    time.sleep(60)
