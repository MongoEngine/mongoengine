from mongoengine import *

class ContentType(Document):
    name = StringField(max_length=100)
    app_label = StringField(max_length=100)
    model = StringField(max_length=100)