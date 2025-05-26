class RecordGeneric:
    def __init__(self, attribute_names):
        for name in attribute_names:
            setattr(self, name, None)



        self._attribute_names = attribute_names

    def to_dict(self):
        return {attr: getattr(self, attr) for attr in self._attribute_names}

    def __str__(self):
        return " || ".join(f"{attr}: {getattr(self, attr)}" for attr in self._attribute_names)
