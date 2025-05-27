import struct
import os
import sys
import numpy as np
from bin_data.Record import RecordGeneric

class KeyHandler:
    def __init__(self, tipo: str, size=50):
        self.tipo = tipo.lower()
        self.size = size

    def serialize(self, key):

        if self.tipo == 'int' or self.tipo == 'serial':
            return struct.pack('i', key)
        elif self.tipo == 'float':
            return struct.pack('f', key)
        elif self.tipo == 'str' or self.tipo == 'text':
            return key.encode('utf-8').ljust(self.size, b'\x00')

    def deserialize(self, data):
        if self.tipo == 'int' or self.tipo == 'serial':
            return struct.unpack('i', data)[0]
        elif self.tipo == 'float':
            return struct.unpack('f', data)[0]
        elif self.tipo == 'str' or self.tipo == 'text':
            return data.rstrip(b'\x00').decode('utf-8')

    def compare(self, a, b):
        return (a > b) - (a < b)


class LeafNode:
    HEADER_FORMAT = 'Bqiq'  # is_leaf (1 byte), parent (8 bytes), n_keys (4 bytes), next_leaf (8 bytes)
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, parent, n_keys, next_leaf, values, key_handler):

        self.is_leaf = True
        self.parent = parent
        self.n_keys = n_keys
        self.next_leaf = next_leaf
        self.values = values  # list of [key, data_pos]
        self.key_handler = key_handler

    def to_bytes(self, order):
        key_size = self.key_handler.size if self.key_handler.tipo == 'str' or self.key_handler.tipo == 'text' else 4
        header = struct.pack(self.HEADER_FORMAT, self.is_leaf, self.parent, self.n_keys, self.next_leaf)
        body = b''
        for i in range(order - 1):
            if i < self.n_keys:
                key = self.key_handler.serialize(self.values[i][0])

                pos = struct.pack('q', self.values[i][1])
            else:
                # Rellenar con clave "vacía" del tipo correcto
                if self.key_handler.tipo == 'int' or self.key_handler.tipo == 'serial':
                    key = self.key_handler.serialize(0)
                elif self.key_handler.tipo == 'float':
                    key = self.key_handler.serialize(0.0)
                elif self.key_handler.tipo == 'str' or self.key_handler.tipo == 'text':
                    key = self.key_handler.serialize("")
                pos = struct.pack('q', -1)

            body += key + pos
        final = header + body
        expected = self.HEADER_SIZE + (key_size + 8) * (order - 1)
        assert len(final) == expected, f"Leaf node size mismatch: {len(final)} vs {expected}"
        return final

    @staticmethod
    def from_bytes(data, order, key_handler):
        header = struct.unpack(LeafNode.HEADER_FORMAT, data[:LeafNode.HEADER_SIZE])
        is_leaf, parent, n_keys, next_leaf = header
        offset = LeafNode.HEADER_SIZE
        values = []
        key_size = key_handler.size if key_handler.tipo == 'str' or key_handler.tipo == 'text' else 4
        for _ in range(n_keys):
            key_bytes = data[offset:offset + key_size]
            key = key_handler.deserialize(key_bytes)
            offset += key_size
            pos = struct.unpack('q', data[offset:offset + 8])[0]
            offset += 8
            values.append([key, pos])
        return LeafNode(parent, n_keys, next_leaf, values, key_handler)

    def __str__(self):
        out = f"LeafNode (parent={self.parent}, next_leaf={self.next_leaf}, keys={self.n_keys})\n"
        for k, p in self.values:
            out += f"  Key: {k}, Pos: {p}\n"
        return out


class InternalNode:
    HEADER_FORMAT = 'Bqi'  # is_leaf (1 byte), parent (8 bytes), n_keys (4 bytes)
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, parent, n_keys, children, keys, key_handler):
        self.is_leaf = False
        self.parent = parent
        self.n_keys = n_keys
        self.children = children  # list of positions
        self.keys = keys  # list of keys
        self.key_handler = key_handler

    def to_bytes(self, order):
        key_size = self.key_handler.size if self.key_handler.tipo == 'str' or self.key_handler.tipo == 'text' else 4
        header = struct.pack(self.HEADER_FORMAT, self.is_leaf, self.parent, self.n_keys)
        body = b''
        # punteros hijos
        for i in range(order):
            ptr = self.children[i] if i < len(self.children) else -1
            body += struct.pack('q', ptr)
        # claves
        for i in range(order - 1):
            if i < len(self.keys):
                key = self.key_handler.serialize(self.keys[i])
            else:
                # Rellenar con clave "vacía" del tipo correcto
                if self.key_handler.tipo == 'int' or self.key_handler.tipo == 'serial':
                    key = self.key_handler.serialize(0)
                elif self.key_handler.tipo == 'float':
                    key = self.key_handler.serialize(0.0)
                elif self.key_handler.tipo == 'str' or self.key_handler.tipo == 'text':
                    key = self.key_handler.serialize("")
            body += key
        final = header + body
        expected = self.HEADER_SIZE + (order * 8) + (key_size * (order - 1))
        assert len(final) == expected, f"Internal node size mismatch: {len(final)} vs {expected}"
        return final

    @staticmethod
    def from_bytes(data, order, key_handler):
        header = struct.unpack(InternalNode.HEADER_FORMAT, data[:InternalNode.HEADER_SIZE])
        is_leaf, parent, n_keys = header
        offset = InternalNode.HEADER_SIZE
        children = []
        for _ in range(order):
            ptr = struct.unpack('q', data[offset:offset + 8])[0]
            offset += 8
            children.append(ptr)
        keys = []
        key_size = key_handler.size if key_handler.tipo == 'str' or key_handler.tipo == 'text' else 4
        for _ in range(order - 1):
            key_bytes = data[offset:offset + key_size]
            key = key_handler.deserialize(key_bytes)
            offset += key_size
            keys.append(key)
        return InternalNode(parent, n_keys, children, keys, key_handler)

    def __str__(self):
        out = f"InternalNode (parent={self.parent}, keys={self.n_keys})\n"
        for i, key in enumerate(self.keys):
            out += f"  Key[{i}]: {key}\n"
        out += f"  Children positions: {self.children}\n"
        return out




class BTreeIndex:
    HEADER_FORMAT = 'qii'  # root_pos (8 bytes), order (4 bytes), record_count (4 bytes)
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    # node_file, data_file, order=4, key_type='str', key_size=50, key_attr_index=0
    def __init__(self, atribute_index, atribute_type, file_name, size_kh = None
                 , data_name = None, records = None,
                 is_create_bin = False, order = 4):


        self.node_file = file_name
        self.data_file = os.path.join(data_name, 'data.databin')
        self.order = order
        self.key_handler = KeyHandler(tipo=atribute_type, size=size_kh)
        self.record_count = 0
        self.root_pos = -1

        if not is_create_bin:

            with open(self.node_file, 'wb') as f:
                f.write(struct.pack(self.HEADER_FORMAT, -1, order, 0))


            # records[0] = cabeceras
            if len(records) > 1:
                for i, record in enumerate(records):
                    self.insert(record.to_dict()[atribute_index.lower()], i)

            print(f"Índice BTREE creado exitosamente en {self.node_file}")

        with open(self.node_file, 'rb') as f:

            header_bytes = f.read(self.HEADER_SIZE)
            if len(header_bytes) == self.HEADER_SIZE:
                self.root_pos, self.order, self.record_count = struct.unpack(self.HEADER_FORMAT, header_bytes)
            else:
                raise IOError("Archivo de nodos corrupto o incompleto")

    # Devuelve el tamaño de un nodo hoja en bytes.
    # Se calcula como el tamaño del encabezado más el tamaño de cada par (clave, puntero a datos),
    # multiplicado por la cantidad máxima de claves (order - 1).
    def leaf_node_size(self):
        key_size = self.key_handler.size if self.key_handler.tipo == 'str' or self.key_handler.tipo == 'text' else 4
        return LeafNode.HEADER_SIZE + (key_size + 8) * (self.order - 1)


    # Devuelve el tamaño de un nodo interno en bytes.
    # Un nodo interno guarda 'order' punteros a hijos y 'order - 1' claves.
    def internal_node_size(self):
        key_size = self.key_handler.size if self.key_handler.tipo == 'str' or self.key_handler.tipo == 'text' else 4
        return InternalNode.HEADER_SIZE + (self.order * 8) + ((self.order - 1) * key_size)

    # Escribe un nodo en una posición específica del archivo de nodos.
    def write_node(self, node, pos):
        with open(self.node_file, 'r+b') as f:
            f.seek(pos)
            f.write(node.to_bytes(self.order))

    # Lee un nodo desde una posición específica del archivo.
    # Determina si es hoja o interno leyendo el primer byte.
    def read_node(self, pos):
        with open(self.node_file, 'rb') as f:
            f.seek(pos)
            tipo_byte = f.read(1)
            if not tipo_byte:
                raise IOError(f"Error leyendo nodo en pos {pos}: archivo incompleto")

            is_leaf = struct.unpack('B', tipo_byte)[0]
            f.seek(pos)

            if is_leaf:
                size = self.leaf_node_size()
            else:
                size = self.internal_node_size()

            data = f.read(size)
            if len(data) != size:
                raise IOError(f"Error leyendo nodo en pos {pos}: esperado {size} bytes, leído {len(data)}")

            if is_leaf:
                return LeafNode.from_bytes(data, self.order, self.key_handler)
            else:
                return InternalNode.from_bytes(data, self.order, self.key_handler)

    # Agrega un nodo nuevo al final del archivo.
    # Actualiza el encabezado del archivo con el nuevo número de registros.
    def append_node(self, node):
        if node.is_leaf:
            node_size = self.leaf_node_size()
        else:
            node_size = self.internal_node_size()

        with open(self.node_file, 'ab') as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell()
        if node.is_leaf:
            node_size = self.leaf_node_size()
        else:
            node_size = self.internal_node_size()

        self.write_node(node, pos)
        self.record_count += 1
        with open(self.node_file, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack(self.HEADER_FORMAT, self.root_pos, self.order, self.record_count))
        return pos

    # Inserta un nuevo registro: guarda los datos en archivo y llama a insert().
    def insert_record(self, key, record: RecordGeneric):
        if key is None:
            key = getattr(record, record._attributes[self.key_attr_index])
        with open(self.data_file, 'ab') as f:
            f.write(record.to_bytes())
        pos = self.record_count
        self.record_count += 1
        self.insert(key, pos)


    # Inserta una clave en el árbol.
    # Si el árbol está vacío, crea una hoja como raíz.
    # Si hay división, crea una nueva raíz.
    def insert(self, key, data_pos):
        if self.root_pos == -1:
            leaf = LeafNode(-1, 1, -1, [[key, data_pos]], self.key_handler)
            self.root_pos = self.append_node(leaf)
            return

        result = self._insert_recursive(self.root_pos, key, data_pos)
        if result is not None:
            new_key, new_pos = result
            old_root = self.read_node(self.root_pos)
            new_root = InternalNode(-1, 1, [self.root_pos, new_pos], [new_key], self.key_handler)
            self.root_pos = self.append_node(new_root)
            with open(self.node_file, 'r+b') as f:
                f.seek(0)
                f.write(struct.pack('q', self.root_pos))

    # Inserción recursiva. Si se divide el nodo, devuelve clave-promoción y nueva posición.
    def _insert_recursive(self, pos, key, data_pos):
        node = self.read_node(pos)

        if node.is_leaf:
            # Permitimos claves duplicadas
            node.values.append([key, data_pos])
            node.values.sort(key=lambda x: (x[0], x[1]))
            if len(node.values) < self.order:
                node.n_keys = len(node.values)
                self.write_node(node, pos)
                return None
            else:
                mid = len(node.values) // 2
                left = node.values[:mid]
                right = node.values[mid:]
                node.values = left
                node.n_keys = len(left)
                new_leaf = LeafNode(-1, len(right), node.next_leaf, right, self.key_handler)
                new_pos = self.append_node(new_leaf)
                node.next_leaf = new_pos
                self.write_node(node, pos)
                return right[0][0], new_pos
        else:
            i = 0
            while i < node.n_keys and self.key_handler.compare(key, node.keys[i]) >= 0:
                i += 1
            result = self._insert_recursive(node.children[i], key, data_pos)
            if result is None:
                return None
            new_key, new_child_pos = result
            node.keys.insert(i, new_key)
            node.children.insert(i + 1, new_child_pos)
            node.n_keys += 1

            if node.n_keys < self.order:
                self.write_node(node, pos)
                return None
            else:
                mid = node.n_keys // 2
                promote_key = node.keys[mid]
                left_keys = node.keys[:mid]
                right_keys = node.keys[mid + 1:]
                left_children = node.children[:mid + 1]
                right_children = node.children[mid + 1:]

                node.keys = left_keys
                node.children = left_children
                node.n_keys = len(left_keys)

                new_internal = InternalNode(-1, len(right_keys), right_children, right_keys, self.key_handler)
                new_pos = self.append_node(new_internal)
                self.write_node(node, pos)
                return promote_key, new_pos

    def print_tree(self, pos=None, nivel=0):
        indent = '  ' * nivel
        if pos is None:
            pos = self.root_pos
        node = self.read_node(pos)
        if node.is_leaf:
            print(f"{indent}[Hoja] Pos: {pos}")
            for k, p in node.values:
                print(f"{indent}  {k} → {p}")
        else:
            print(f"{indent}[Interno] Pos: {pos}")
            for i in range(node.n_keys):
                self.print_tree(node.children[i], nivel + 1)
                print(f"{indent}  Key[{i}]: {node.keys[i]}")
            self.print_tree(node.children[node.n_keys], nivel + 1)

    # Busca una clave en el árbol. Devuelve todas las ocurrencias.
    def search(self, key):
        if self.root_pos == -1:
            return []
        return self._search_in_leaf(self.root_pos, key)

    # Búsqueda recursiva hasta llegar a una hoja.
    def _search_in_leaf(self, pos, key):
        node = self.read_node(pos)
        if node.is_leaf:
            return [pair for pair in node.values if self.key_handler.compare(pair[0], key) == 0]
        else:
            i = 0
            while i < node.n_keys and self.key_handler.compare(key, node.keys[i]) >= 0:
                i += 1
            return self._search_in_leaf(node.children[i], key)

    # Búsqueda de claves entre start_key y end_key (inclusive).
    def range_search(self, start_key, end_key):
        if self.root_pos == -1:
            return []
        results = []
        self._range_collect(self.root_pos, start_key, end_key, results)
        return results

    # Recolecta recursivamente todos los pares dentro del rango en hojas consecutivas.
    def _range_collect(self, pos, start_key, end_key, results):
        node = self.read_node(pos)
        if node.is_leaf:
            for key, value in node.values:
                if self.key_handler.compare(start_key, key) <= 0 <= self.key_handler.compare(end_key, key):
                    results.append((key, value))
            next_pos = node.next_leaf
            if next_pos != -1:
                self._range_collect(next_pos, start_key, end_key, results)
        else:
            i = 0
            while i < node.n_keys and self.key_handler.compare(start_key, node.keys[i]) > 0:
                i += 1
            self._range_collect(node.children[i], start_key, end_key, results)
        
    # Elimina una clave del árbol.
    def delete(self, key):
        if self.root_pos == -1:
            return False
        changed, shrink, new_root = self._delete_recursive(self.root_pos, key)
        if shrink and new_root is not None:
            self.root_pos = new_root
            with open(self.node_file, 'r+b') as f:
                f.seek(0)
                f.write(struct.pack('q', self.root_pos))
        return changed

    # Eliminación recursiva. Puede provocar redistribución o fusión de nodos.
    def _delete_recursive(self, pos, key):
        node = self.read_node(pos)
        if node.is_leaf:
            original_len = len(node.values)
            node.values = [pair for pair in node.values if self.key_handler.compare(pair[0], key) != 0]
            node.n_keys = len(node.values)
            self.write_node(node, pos)
            changed = original_len != node.n_keys
            new_min_key = node.values[0][0] if node.n_keys > 0 else None
            return changed, node.n_keys == 0, new_min_key

        i = 0
        while i < node.n_keys and self.key_handler.compare(key, node.keys[i]) >= 0:
            i += 1
        changed, shrink, new_min_key = self._delete_recursive(node.children[i], key)
        if not changed:
            return False, False, None
        if new_min_key is not None and i < node.n_keys:
            if self.key_handler.compare(node.keys[i], new_min_key) != 0:
                node.keys[i] = new_min_key
        if shrink:
            shrink, new_child = self._redistribute_or_merge(node, pos, i)
            if shrink:
                return True, True, new_child
        self.write_node(node, pos)
        return True, False, None

    # Intenta redistribuir o fusionar un hijo que se quedó con pocos elementos.
    def _redistribute_or_merge(self, node, pos, i):
        left_idx = i - 1
        right_idx = i + 1
        left = self.read_node(node.children[left_idx]) if i > 0 else None
        right = self.read_node(node.children[right_idx]) if i + 1 <= node.n_keys else None
        target = self.read_node(node.children[i])
        if right and right.n_keys > (self.order - 1) // 2:
            self._redistribute_from_right(node, target, right, i)
            self.write_node(target, node.children[i])
            self.write_node(right, node.children[i + 1])
            return False, None
        elif left and left.n_keys > (self.order - 1) // 2:
            self._redistribute_from_left(node, target, left, i)
            self.write_node(target, node.children[i])
            self.write_node(left, node.children[i - 1])
            return False, None
        else:
            if right:
                self._merge_with_right(node, target, right, i)
                self.write_node(target, node.children[i])
                node.keys.pop(i)
                node.children.pop(i + 1)
                node.n_keys -= 1
            elif left:
                self._merge_with_left(node, target, left, i)
                self.write_node(left, node.children[i - 1])
                node.keys.pop(i - 1)
                node.children.pop(i)
                node.n_keys -= 1
            self.write_node(node, pos)
            if node.n_keys == 0:
                return True, node.children[0]
            return False, None

    # Redistribuye desde el hermano derecho.
    def _redistribute_from_right(self, parent, target, right, i):
        if target.is_leaf:
            target.values.append(right.values.pop(0))
            target.n_keys += 1
            right.n_keys -= 1
            parent.keys[i] = right.values[0][0]
        else:
            target.keys.append(parent.keys[i])
            parent.keys[i] = right.keys.pop(0)
            target.children.append(right.children.pop(0))
            target.n_keys += 1
            right.n_keys -= 1

    # Redistribuye desde el hermano izquierdo.
    def _redistribute_from_left(self, parent, target, left, i):
        if target.is_leaf:
            target.values.insert(0, left.values.pop())
            target.n_keys += 1
            left.n_keys -= 1
            parent.keys[i - 1] = target.values[0][0]
        else:
            target.keys.insert(0, parent.keys[i - 1])
            parent.keys[i - 1] = left.keys.pop()
            target.children.insert(0, left.children.pop())
            target.n_keys += 1
            left.n_keys -= 1

    # Fusiona con el hermano derecho.
    def _merge_with_right(self, parent, target, right, i):
        if target.is_leaf:
            target.values.extend(right.values)
            target.n_keys = len(target.values)
            target.next_leaf = right.next_leaf
        else:
            target.keys.append(parent.keys[i])
            target.keys.extend(right.keys)
            target.children.extend(right.children)
            target.n_keys = len(target.keys)

    # Fusiona con el hermano izquierdo.
    def _merge_with_left(self, parent, target, left, i):
        if target.is_leaf:
            left.values.extend(target.values)
            left.n_keys = len(left.values)
            left.next_leaf = target.next_leaf
        else:
            left.keys.append(parent.keys[i - 1])
            left.keys.extend(target.keys)
            left.children.extend(target.children)
            left.n_keys = len(left.keys)

    # Eliminación
    def delete(self, key):
        if self.root_pos == -1:
            return False
        changed, shrink, new_root = self._delete_recursive(self.root_pos, key)
        if shrink and new_root is not None:
            self.root_pos = new_root
            with open(self.node_file, 'r+b') as f:
                f.seek(0)
                f.write(struct.pack('q', self.root_pos))
        return changed

    def _delete_recursive(self, pos, key):
        node = self.read_node(pos)

        if node.is_leaf:
            original_len = len(node.values)
            node.values = [pair for pair in node.values if self.key_handler.compare(pair[0], key) != 0]
            node.n_keys = len(node.values)
            self.write_node(node, pos)

            changed = original_len != node.n_keys
            new_min_key = node.values[0][0] if node.n_keys > 0 else None
            return changed, node.n_keys == 0, new_min_key

        i = 0
        while i < node.n_keys and self.key_handler.compare(key, node.keys[i]) >= 0:
            i += 1

        changed, shrink, new_min_key = self._delete_recursive(node.children[i], key)

        if not changed:
            return False, False, None

        # Actualizar la clave separadora si es necesario
        if new_min_key is not None and i < node.n_keys:
            if self.key_handler.compare(node.keys[i], new_min_key) != 0:
                node.keys[i] = new_min_key

        if shrink:
            shrink, new_child = self._redistribute_or_merge(node, pos, i)
            if shrink:
                return True, True, new_child

        self.write_node(node, pos)
        return True, False, None

    def _redistribute_or_merge(self, node, pos, i):
        left_idx = i - 1
        right_idx = i + 1
        left = self.read_node(node.children[left_idx]) if i > 0 else None
        right = self.read_node(node.children[right_idx]) if i + 1 <= node.n_keys else None
        target = self.read_node(node.children[i])

        if right and right.n_keys > (self.order - 1) // 2:
            self._redistribute_from_right(node, target, right, i)
            self.write_node(target, node.children[i])
            self.write_node(right, node.children[i + 1])
            return False, None
        elif left and left.n_keys > (self.order - 1) // 2:
            self._redistribute_from_left(node, target, left, i)
            self.write_node(target, node.children[i])
            self.write_node(left, node.children[i - 1])
            return False, None
        else:
            if right:
                self._merge_with_right(node, target, right, i)
                self.write_node(target, node.children[i])
                node.keys.pop(i)
                node.children.pop(i + 1)
                node.n_keys -= 1
            elif left:
                self._merge_with_left(node, target, left, i)
                self.write_node(left, node.children[i - 1])
                node.keys.pop(i - 1)
                node.children.pop(i)
                node.n_keys -= 1
            self.write_node(node, pos)
            if node.n_keys == 0:
                return True, node.children[0]
            return False, None

    def _redistribute_from_right(self, parent, target, right, i):
        if target.is_leaf:
            target.values.append(right.values.pop(0))
            target.n_keys += 1
            right.n_keys -= 1
            parent.keys[i] = right.values[0][0]
        else:
            target.keys.append(parent.keys[i])
            parent.keys[i] = right.keys.pop(0)
            target.children.append(right.children.pop(0))
            target.n_keys += 1
            right.n_keys -= 1

    def _redistribute_from_left(self, parent, target, left, i):
        if target.is_leaf:
            target.values.insert(0, left.values.pop())
            target.n_keys += 1
            left.n_keys -= 1
            parent.keys[i - 1] = target.values[0][0]
        else:
            target.keys.insert(0, parent.keys[i - 1])
            parent.keys[i - 1] = left.keys.pop()
            target.children.insert(0, left.children.pop())
            target.n_keys += 1
            left.n_keys -= 1

    def _merge_with_right(self, parent, target, right, i):
        if target.is_leaf:
            target.values.extend(right.values)
            target.n_keys = len(target.values)
            target.next_leaf = right.next_leaf
        else:
            target.keys.append(parent.keys[i])
            target.keys.extend(right.keys)
            target.children.extend(right.children)
            target.n_keys = len(target.keys)

    def _merge_with_left(self, parent, target, left, i):
        if target.is_leaf:
            left.values.extend(target.values)
            left.n_keys = len(left.values)
            left.next_leaf = target.next_leaf
        else:
            left.keys.append(parent.keys[i - 1])
            left.keys.extend(target.keys)
            left.children.extend(target.children)
            left.n_keys = len(left.keys)


