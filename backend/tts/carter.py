from gtts import gTTS
from playsound import playsound
import os

def say(text):
    audio = gTTS(text)
    audio.save('current_say_msg.mp3')
    playsound('current_say_msg.mp3')
    os.remove('current_say_msg.mp3')
          

say("hi there")