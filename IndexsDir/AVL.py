import struct
import os


class KeyHandler:
    def __init__(self, tipo: str, size=20):
        self.tipo = tipo.lower()
        self.size = size

    def serialize(self, key):

        if self.tipo == 'int' or self.tipo == 'serial':
            return struct.pack('i', key)
        elif self.tipo == 'float':
            return struct.pack('f', key)
        elif self.tipo == 'str'or self.tipo == 'text':
            return key.encode('utf-8').ljust(self.size, b'\x00')

    def deserialize(self, data):
        if self.tipo == 'int' or self.tipo == 'serial':
            return struct.unpack('i', data)[0]
        elif self.tipo == 'float':
            return struct.unpack('f', data)[0]
        elif self.tipo == 'str'or self.tipo == 'text':
            return data.rstrip(b'\x00').decode('utf-8')

    def compare(self, a, b):
        return (a > b) - (a < b)


class IndexNode:
    def __init__(self, key, pos, left=-1, right=-1, height=1):
        self.key = key
        self.pos = pos
        self.left = left
        self.right = right
        self.height = height

    def to_bytes(self, key_handler: KeyHandler):
        key_bytes = key_handler.serialize(self.key)
        return key_bytes + struct.pack('iiii', self.pos, self.left, self.right, self.height)

    @staticmethod
    def from_bytes(data: bytes, key_handler: KeyHandler):
        key_size = key_handler.size if key_handler.tipo == 'str' or key_handler.tipo == 'text' else 4
        key = key_handler.deserialize(data[:key_size])
        pos, left, right, height = struct.unpack('iiii', data[key_size:])
        return IndexNode(key, pos, left, right, height)


class AVLIndex:

    def __init__(self, atribute_index, atribute_type, file_name, data_name = None,
                size_kh = None, records = None, is_create_bin = False):

            self.key_handler = KeyHandler(tipo=atribute_type, size = size_kh)
            self.filename = file_name
            self.record_size = (self.key_handler.size if self.key_handler.tipo == 'str' or self.key_handler.tipo == 'text' else 4) + 16

            if not is_create_bin:

                with open(self.filename, "wb") as f:
                    f.write(struct.pack("i", -1))

                # records[0] = cabeceras
                if len(records) > 1:
                    for i, record in enumerate(records):
                        self.insert(record.to_dict()[atribute_index.lower()], i)

                print(f"Índice AVL creado exitosamente en {self.filename}")


    def get_root(self):
        with open(self.filename, "rb") as f:
            return struct.unpack("i", f.read(4))[0]

    def set_root(self, pos):
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(struct.pack("i", pos))

    def read_node(self, pos, file):
        offset = 4 + pos * self.record_size
        file.seek(offset)
        return IndexNode.from_bytes(file.read(self.record_size), self.key_handler)

    def write_node(self, pos, node, file):
        offset = 4 + pos * self.record_size
        file.seek(offset)
        file.write(node.to_bytes(self.key_handler))

    def get_height(self, pos, file):
        return self.read_node(pos, file).height if pos != -1 else 0

    def balance_factor(self, node, file):
        return self.get_height(node.left, file) - self.get_height(node.right, file)

    def rotate_right(self, file, pos):
        y = self.read_node(pos, file)
        x = self.read_node(y.left, file)
        x_pos = y.left
        y.left = x.right
        x.right = pos
        y.height = 1 + max(self.get_height(y.left, file), self.get_height(y.right, file))
        x.height = 1 + max(self.get_height(x.left, file), self.get_height(x.right, file))
        self.write_node(pos, y, file)
        self.write_node(x_pos, x, file)
        return x_pos

    def rotate_left(self, file, pos):
        x = self.read_node(pos, file)
        y = self.read_node(x.right, file)
        y_pos = x.right
        x.right = y.left
        y.left = pos
        x.height = 1 + max(self.get_height(x.left, file), self.get_height(x.right, file))
        y.height = 1 + max(self.get_height(y.left, file), self.get_height(y.right, file))
        self.write_node(pos, x, file)
        self.write_node(y_pos, y, file)
        return y_pos

    def balance(self, file, pos):
        node = self.read_node(pos, file)
        bf = self.balance_factor(node, file)

        if bf > 1:
            left = self.read_node(node.left, file)
            if self.balance_factor(left, file) < 0:
                node.left = self.rotate_left(file, node.left)
                self.write_node(pos, node, file)
            return self.rotate_right(file, pos)

        if bf < -1:
            right = self.read_node(node.right, file)
            if self.balance_factor(right, file) > 0:
                node.right = self.rotate_right(file, node.right)
                self.write_node(pos, node, file)
            return self.rotate_left(file, pos)

        return pos

    def insert(self, key, pos_dato):
        with open(self.filename, "r+b") as file:
            root = self.get_root()
            new_root = self._insert_rec(file, root, key, pos_dato)
            self.set_root(new_root)

    def _insert_rec(self, file, pos, key, pos_dato):
        if pos == -1:
            file.seek(0, 2)
            new_pos = (file.tell() - 4) // self.record_size
            node = IndexNode(key, pos_dato)
            self.write_node(new_pos, node, file)
            return new_pos

        node = self.read_node(pos, file)
        cmp = self.key_handler.compare(key, node.key)

        if cmp < 0:
            node.left = self._insert_rec(file, node.left, key, pos_dato)
        elif cmp >= 0:
            node.right = self._insert_rec(file, node.right, key, pos_dato)
        else:
            raise ValueError(f"Clave duplicada: {key}")

        node.height = 1 + max(self.get_height(node.left, file), self.get_height(node.right, file))
        self.write_node(pos, node, file)
        return self.balance(file, pos)

    # Search
    def search(self, key):
        with open(self.filename, "rb") as file:
            return self._search_rec(file, self.get_root(), key)

    def _search_rec(self, file, pos, key):
        if pos == -1:
            return None
        node = self.read_node(pos, file)
        cmp = self.key_handler.compare(key, node.key)
        if cmp == 0:
            return node
        elif cmp < 0:
            return self._search_rec(file, node.left, key)
        else:
            return self._search_rec(file, node.right, key)

    # Range search
    def range_search(self, key_low, key_high):
        results = []
        with open(self.filename, "rb") as file:
            self._range_search_rec(file, self.get_root(), key_low, key_high, results)
        return results

    def _range_search_rec(self, file, pos, key_low, key_high, results):
        if pos == -1:
            return
        node = self.read_node(pos, file)
        if self.key_handler.compare(key_low, node.key) < 0:
            self._range_search_rec(file, node.left, key_low, key_high, results)
        if self.key_handler.compare(key_low, node.key) <= 0 and self.key_handler.compare(node.key, key_high) <= 0:
            results.append((node.key, node.pos))
        if self.key_handler.compare(node.key, key_high) < 0:
            self._range_search_rec(file, node.right, key_low, key_high, results)

    # Delete
    def delete(self, key):
        with open(self.filename, "r+b") as file:
            root = self.get_root()
            new_root = self._delete_rec(file, root, key)
            self.set_root(new_root)

    def _delete_rec(self, file, pos, key):
        if pos == -1:
            return -1

        node = self.read_node(pos, file)
        cmp = self.key_handler.compare(key, node.key)

        if cmp < 0:
            node.left = self._delete_rec(file, node.left, key)
        elif cmp > 0:
            node.right = self._delete_rec(file, node.right, key)
        else:
            if node.left == -1:
                return node.right
            elif node.right == -1:
                return node.left

            min_larger_pos, min_larger_node = self._get_min_node(file, node.right)
            node.key, node.pos = min_larger_node.key, min_larger_node.pos
            node.right = self._delete_rec(file, node.right, min_larger_node.key)

        node.height = 1 + max(self.get_height(node.left, file), self.get_height(node.right, file))
        self.write_node(pos, node, file)
        return self.balance(file, pos)

    def _get_min_node(self, file, pos):
        current_pos = pos
        current_node = self.read_node(current_pos, file)
        while current_node.left != -1:
            current_pos = current_node.left
            current_node = self.read_node(current_pos, file)
        return current_pos, current_node


def create_index_avl(records, atribute_index, type, file_name):
    try:
        KEY_HANDLER = KeyHandler(tipo=type)  # tipo del key
        FILENAME = file_name
        avl = AVLIndex(FILENAME, KEY_HANDLER)
        for i, record in enumerate(records):
            avl.insert(record.to_dict()[atribute_index], i)
        print(f"Índice AVL creado exitosamente en {FILENAME}")
    except Exception as e:
        print(f"Error al crear el índice AVL: {e}")
