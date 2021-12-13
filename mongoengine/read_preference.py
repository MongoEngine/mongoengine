class RPReadPreferenceContext(object):
    read_preferences = []

    def __init__(self, read_preference):
        self.current_read_preference = read_preference

    def __enter__(self):
        RPReadPreferenceContext.read_preferences.append(self.current_read_preference)

    @classmethod
    def get_read_preference(cls):
        if len(cls.read_preferences):
            return cls.read_preferences[-1]
        return None

    def __exit__(self, *args):
        RPReadPreferenceContext.read_preferences.pop()
