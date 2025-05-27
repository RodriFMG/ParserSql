import os
import hashlib
import struct
from .config import GLOBAL_DEPTH_MAX
from .bucket import Bucket

DIRECTORY_ENTRY_SIZE = struct.calcsize("Q")  # Cada entrada del directorio es un offset de 8 bytes


class KeyHandler:
    def __init__(self, tipo: str, size=20):
        self.tipo = tipo.lower()
        self.size = size

    def serialize(self, key):
        if self.tipo == "int" or self.tipo == "serial":
            return struct.pack("i", int(key))
        elif self.tipo == "float":
            return struct.pack("f", float(key))
        elif self.tipo == "str" or self.tipo == "text":
            return str(key).encode('utf-8').ljust(self.size, b'\x00')

    def compare(self, a, b):
        if self.tipo == "int":
            return (int(a) > int(b)) - (int(a) < int(b))
        elif self.tipo == "float":
            return (float(a) > float(b)) - (float(a) < float(b))
        else:
            return (str(a) > str(b)) - (str(a) < str(b))


class ExtendibleHashingIndex:
    def __init__(self, atribute_index, atribute_type, file_name, size_kh = None, records=None, is_create_bin=False
                 , data_name = False, order = None):

        self.atribute_index = atribute_index.lower()
        self.atribute_type = atribute_type
        self.file_name = file_name
        self.key_handler = KeyHandler(atribute_type, size=size_kh)
        self.record_size = 24 if atribute_type == "str" else struct.calcsize("i") + 4
        self.record_format = f"{self.record_size - 4}s i"
        self.global_depth = 2

        create_path_att = os.path.join(data_name, atribute_index)

        os.makedirs(create_path_att, exist_ok=True)

        self.BUCKETS_PATH = os.path.join(create_path_att, "buckets.dat")
        self.DIRECTORY_PATH = os.path.join(create_path_att, "directory.dat")

        #BUCKETS_PATH = file_name + atribute_index

        self.init_files()

        if not is_create_bin:

            if len(records) > 1:
                for i, record in enumerate(records):
                    key = record.to_dict()[self.atribute_index]
                    # print("//" * 20)
                    # print(f"🔑 Insertando clave: {key} (pos: {i})")
                    self.insert(key, i)
                    #self.print_ll()  // print info extendible
                    # print("//" * 20)

    def hash_key(self, key):
        key_bytes = self.key_handler.serialize(key)
        h = hashlib.sha256(key_bytes).hexdigest()
        return bin(int(h, 16))[2:].zfill(256)

    def read_directory(self, path):
        with open(path, "rb") as f:
            data = f.read()
            return [struct.unpack("Q", data[i:i + DIRECTORY_ENTRY_SIZE])[0]
                    for i in range(0, len(data), DIRECTORY_ENTRY_SIZE)]

    def write_directory(self, path, offsets):
        with open(path, "wb") as f:
            for offset in offsets:
                f.write(struct.pack("Q", offset))

    def init_files(self):
        os.makedirs(os.path.dirname(self.BUCKETS_PATH), exist_ok=True)

        bucket_size = len(Bucket(self.record_format, self.record_size).pack())

        if not os.path.exists(self.BUCKETS_PATH):
            with open(self.BUCKETS_PATH, "wb") as f:
                for _ in range(2 ** self.global_depth):
                    b = Bucket(self.record_format, self.record_size)
                    b.local_depth = self.global_depth
                    f.write(b.pack())

        if not os.path.exists(self.DIRECTORY_PATH):
            with open(self.DIRECTORY_PATH, "wb") as f:
                for i in range(2 ** self.global_depth):
                    offset = i * bucket_size
                    f.write(struct.pack("Q", offset))

    def get_bucket_offset(self, key_bits):
        directory = self.read_directory(self.DIRECTORY_PATH)
        index = int(key_bits[-self.global_depth:], 2)
        return index, directory[index]

    def read_bucket(self, offset):
        with open(self.BUCKETS_PATH, "rb") as f:
            f.seek(offset)
            data = f.read(len(Bucket(self.record_format, self.record_size).pack()))
            return Bucket.unpack(data, self.record_format, self.record_size)

    def write_bucket(self, bucket, offset):
        with open(self.BUCKETS_PATH, "r+b") as f:
            f.seek(offset)
            f.write(bucket.pack())

    def insert(self, key, pos):
        key_bits = self.hash_key(key)
        index, offset = self.get_bucket_offset(key_bits)
        bucket = self.read_bucket(offset)

        # Intenta insertar
        if bucket.insert((key, pos)):
            self.write_bucket(bucket, offset)
            return

        # Si hay overflow permitido
        if bucket.local_depth >= GLOBAL_DEPTH_MAX:
            while bucket.next_bucket != -1:
                offset = bucket.next_bucket
                bucket = self.read_bucket(offset)
                if bucket.insert((key, pos)):
                    self.write_bucket(bucket, offset)
                    return

            # Crear nuevo bucket encadenado
            new_bucket = Bucket(self.record_format, self.record_size)
            new_bucket.local_depth = bucket.local_depth  # misma profundidad
            new_offset = os.path.getsize(self.BUCKETS_PATH)

            if new_bucket.insert((key, pos)):
                bucket.next_bucket = new_offset
                self.write_bucket(bucket, offset)  # actualizar puntero
                self.write_bucket(new_bucket, new_offset)
                #print(f"[🧩] Clave {key} insertada en bucket de overflow offset={new_offset}")
            else:
                print("[⛔] Error: no se pudo insertar en nuevo bucket de overflow")
            return

        # Caso normal: intentar split
        #print(f"[🔄] Bucket lleno en offset {offset}, intentando dividir...")
        self.split_bucket(index, offset, bucket)
        self.insert(key, pos)  # Reintento tras split

    def split_bucket(self, index, offset, full_bucket):
        full_bucket.local_depth += 1
        local_depth = full_bucket.local_depth
        directory = self.read_directory(self.DIRECTORY_PATH)

        """
        print(f"\n[🔧 DEBUG] split_bucket(): index={index}, offset={offset}")
        print(f"→ Nuevo local_depth: {local_depth}")
        print(f"→ Profundidad global actual: {self.global_depth}")
        print(f"→ GLOBAL_DEPTH_MAX permitido: {GLOBAL_DEPTH_MAX}")
        """
        if local_depth > GLOBAL_DEPTH_MAX:
            print(f"[⛔] Profundidad local {local_depth} supera GLOBAL_DEPTH_MAX ({GLOBAL_DEPTH_MAX})")
            return False

        if local_depth > self.global_depth:
            self.global_depth += 1
            directory += directory
            print(f"[🔁] Directorio duplicado → nueva profundidad global: {self.global_depth}")

        new_bucket = Bucket(self.record_format, self.record_size)
        new_bucket.local_depth = local_depth
        new_offset = os.path.getsize(self.BUCKETS_PATH)
        print(f"→ Nuevo bucket creado en offset: {new_offset}")

        # Redistribuir registros
        all_records = full_bucket.records.copy()
        full_bucket.records.clear()
        full_bucket.size = 0
        new_bucket.size = 0

        #print(f"[DEBUG] Redistribuyendo {len(all_records)} registros:")
        for key, pos in all_records:
            bits = self.hash_key(key)
            idx = int(bits[-local_depth:], 2)
            destino = "new" if idx & (1 << (local_depth - 1)) else "original"
            print(f"  - Key: {key}, bits: {bits[-local_depth:]}, destino: {destino}")
            if destino == "new":
                new_bucket.insert((key, pos))
            else:
                full_bucket.insert((key, pos))

        # Actualizar directorio
        #print(f"[DEBUG] Actualizando entradas del directorio...")
        for i in range(len(directory)):
            bits = bin(i)[2:].zfill(self.global_depth)
            match = int(bits[-local_depth:], 2) & (1 << (local_depth - 1))
            if match:
                if directory[i] == offset:
                    directory[i] = new_offset
                    #print(f"  - Directorio[{i}] actualizado a nuevo offset {new_offset}")
            else:
                if directory[i] == offset:
                    directory[i] = offset  # redundante pero explícito

        # Guardar en archivo
        self.write_bucket(full_bucket, offset)
        self.write_bucket(new_bucket, new_offset)
        self.write_directory(self.DIRECTORY_PATH, directory)

        #print(f"[✅] Split completado con éxito para offset {offset}\n")
        return True

    def search(self, key):
        key_bits = self.hash_key(key)
        _, offset = self.get_bucket_offset(key_bits)

        while offset != -1:
            bucket = self.read_bucket(offset)
            for k, pos in bucket.records:
                if self.key_handler.compare(k, key) == 0:
                    return pos
            offset = bucket.next_bucket  # Avanza al siguiente bucket si hay
        return None

    def print_ll(self):
        print("\n📂 Directorio y Buckets")
        print(f"🌐 Profundidad global: {self.global_depth}")
        print("=" * 50)

        directory = self.read_directory(self.DIRECTORY_PATH)
        seen_offsets = {}
        for i, offset in enumerate(directory):
            binary_index = bin(i)[2:].zfill(self.global_depth)
            print(f"[{binary_index}] → offset {offset}")

            # Recorrer cadena de buckets encadenados
            current_offset = offset
            while current_offset != -1 and current_offset not in seen_offsets:
                bucket = self.read_bucket(current_offset)
                seen_offsets[current_offset] = bucket
                current_offset = bucket.next_bucket

        print("\n🪣 Buckets:")
        for offset in sorted(seen_offsets.keys()):
            bucket = seen_offsets[offset]
            print(
                f"Offset {offset} | size: {bucket.size}, local_depth: {bucket.local_depth}, next_bucket: {bucket.next_bucket}")
            for key, pos in bucket.records:
                print(f"  - Key: {key} | Pos: {pos}")
            print("-" * 50)
