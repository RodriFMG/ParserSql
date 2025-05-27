import struct

class RecordGeneric:
    def __init__(self, attribute_names):
        for name in attribute_names:
            setattr(self, name, None)
        self._attribute_names = attribute_names

    def to_dict(self):
        return {attr: getattr(self, attr) for attr in self._attribute_names}

    def __str__(self):
        return " || ".join(f"{attr}: {getattr(self, attr)}" for attr in self._attribute_names)

    def to_bytes(self):
        """
        Convierte los atributos en un bloque de bytes. Usa:
        - 'i' para int y 'serial'
        - 'f' para float
        - '50s' para text / str
        """
        format_str = ''
        values = []

        for attr in self._attribute_names:
            val = getattr(self, attr)

            # Inferencia de tipo lógica extendida
            if isinstance(val, int):
                format_str += 'i'
                values.append(val)
            elif isinstance(val, float):
                format_str += 'f'
                values.append(val)
            elif isinstance(val, str) or val is None:
                format_str += '50s'
                val = val if val is not None else ''
                values.append(val.encode('utf-8')[:50].ljust(50, b'\x00'))
            else:
                # Casos como 'serial' (que podría venir como string 'serial', pero con valor int)
                try:
                    val_int = int(val)
                    format_str += 'i'
                    values.append(val_int)
                except:
                    format_str += '50s'
                    val = str(val)
                    values.append(val.encode('utf-8')[:50].ljust(50, b'\x00'))

        return struct.pack(format_str, *values)

