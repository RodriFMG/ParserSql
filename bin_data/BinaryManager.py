import os
import struct
import hashlib
import json
from datetime import date, datetime


class BinStorageManager:
    def __init__(self, bin_dir='bin_data/Tablas/'):
        self.bin_dir = bin_dir
        self.meta_file = os.path.join(bin_dir, "meta.json")
        os.makedirs(bin_dir, exist_ok=True)
        self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(self.meta_file):
            with open(self.meta_file, 'r') as f:
                self.meta = json.load(f)
        else:
            self.meta = {}

    def _save_metadata(self):
        with open(self.meta_file, 'w') as f:
            json.dump(self.meta, f, indent=4)

    def _get_table_path(self, table_name):
        return os.path.join(self.bin_dir, f"{table_name.lower()}.bin")

    def _compute_hash(self, rows):
        """
        Calcula un hash MD5 del contenido de la tabla (solo datos).
        Convierte automáticamente fechas a string ISO para evitar errores de serialización.
        """

        def _default_serializer(obj):
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()
            raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

        row_str = json.dumps(rows, sort_keys=True, default=_default_serializer).encode('utf-8')
        return hashlib.md5(row_str).hexdigest()

    def save_table(self, table_name, rows, header=None):
        """Guarda los datos de la tabla en un archivo binario, incluyendo encabezado"""
        file_path = self._get_table_path(table_name)

        # Si ya está sincronizado, no sobrescribas
        if os.path.exists(file_path) and self.is_synced(table_name, rows):
            print(f"[BIN] No se modificó '{table_name}.bin', está sincronizado.")
            return

        with open(file_path, 'wb') as f:
            if not header:
                if not rows:
                    raise ValueError("No se puede guardar tabla vacía sin encabezado")
                header = list(rows[0].keys())

            # Guardar encabezado
            encoded_header = [s.encode('utf-8') for s in header]
            f.write(struct.pack('I', len(encoded_header)))
            for col in encoded_header:
                f.write(struct.pack('I', len(col)))
                f.write(col)

            # Guardar filas
            for row in rows:
                values = [str(row.get(col, '')).encode('utf-8') for col in header]
                total_len = sum(4 + len(val) for val in values)
                f.write(struct.pack('I', total_len))
                for val in values:
                    f.write(struct.pack('I', len(val)))
                    f.write(val)

        # Actualizar metadatos
        self.meta[table_name.lower()] = {
            "hash": self._compute_hash(rows),
            "columns": header,
            "last_modified": datetime.now().isoformat()
        }
        self._save_metadata()

    def load_table(self, table_name):
        """Carga la tabla desde el archivo binario, devolviendo una lista de diccionarios"""
        file_path = self._get_table_path(table_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No existe archivo binario para '{table_name}'")

        rows = []
        with open(file_path, 'rb') as f:
            num_cols = struct.unpack('I', f.read(4))[0]
            header = []
            for _ in range(num_cols):
                col_len = struct.unpack('I', f.read(4))[0]
                col_name = f.read(col_len).decode('utf-8')
                header.append(col_name)

            while True:
                len_bytes = f.read(4)
                if not len_bytes:
                    break
                total_len = struct.unpack('I', len_bytes)[0]
                end = f.tell() + total_len

                values = []
                while f.tell() < end:
                    val_len = struct.unpack('I', f.read(4))[0]
                    val = f.read(val_len).decode('utf-8')
                    values.append(val)

                row = dict(zip(header, values))
                rows.append(row)

        return rows

    def is_synced(self, table_name, rows):
        """Verifica si el contenido de la tabla en memoria coincide con el último guardado"""
        current_hash = self._compute_hash(rows)
        saved_info = self.meta.get(table_name.lower(), {})
        return saved_info.get("hash") == current_hash
