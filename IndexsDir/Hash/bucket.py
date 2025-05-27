import struct
from .config import BLOCK_FACTOR

# Header incluye: tamaño actual, siguiente bucket y profundidad local
BUCKET_HEADER_FORMAT = "iii"  # size, next_bucket, local_depth
BUCKET_HEADER_SIZE = struct.calcsize(BUCKET_HEADER_FORMAT)

class Bucket:
    def __init__(self, record_format, record_size):
        self.record_format = record_format  # Formato de struct: key + pos
        self.record_size = record_size      # Tamaño del record total (bytes)
        self.records = []                   # Lista de tuplas (key, pos)
        self.size = 0                       # Número actual de registros
        self.local_depth = 0                # Profundidad local del bucket
        self.next_bucket = -1              # Encadenamiento si se alcanza GLOBAL_DEPTH_MAX (opcional futuro)

    def insert(self, record):
        """Intenta insertar un registro si hay espacio."""
        if self.size < BLOCK_FACTOR:
            self.records.append(record)
            self.size += 1
            return True
        return False

    def pack(self):
        """Serializa el bucket para almacenarlo en disco."""
        data = struct.pack(BUCKET_HEADER_FORMAT, self.size, self.next_bucket, self.local_depth)
        for key, pos in self.records:
            if not isinstance(key, bytes):
                key = str(key).encode('utf-8')
            key = key.ljust(self.record_size - 4, b'\x00')  # Padding de clave
            data += struct.pack(self.record_format, key, pos)

        # Rellenar espacios vacíos
        for _ in range(BLOCK_FACTOR - self.size):
            empty_key = b'\x00' * (self.record_size - 4)
            data += struct.pack(self.record_format, empty_key, -1)
        return data

    @staticmethod
    def unpack(data_bytes, record_format, record_size):
        """Reconstruye un bucket desde una secuencia de bytes leída del archivo."""
        if len(data_bytes) < BUCKET_HEADER_SIZE:
            raise ValueError(f"[ERROR] Datos insuficientes: {len(data_bytes)} bytes")

        size, next_bucket, local_depth = struct.unpack(BUCKET_HEADER_FORMAT, data_bytes[:BUCKET_HEADER_SIZE])
        records = []
        offset = BUCKET_HEADER_SIZE

        for _ in range(size):
            rec_data = data_bytes[offset:offset + record_size]
            key, pos = struct.unpack(record_format, rec_data)
            if isinstance(key, bytes):
                key = key.rstrip(b'\x00').decode()
            records.append((key, pos))
            offset += record_size

        bucket = Bucket(record_format, record_size)
        bucket.records = records
        bucket.size = size
        bucket.next_bucket = next_bucket
        bucket.local_depth = local_depth
        return bucket

    def print_bucket(self):
        """Imprime el contenido del bucket."""
        print(f"Bucket(size={self.size}, local_depth={self.local_depth})")
        for key, pos in self.records:
            print(f"-- key={key}, pos={pos} --")

    def __repr__(self):
        return f"Bucket(size={self.size}, local_depth={self.local_depth}, records={self.records})"
