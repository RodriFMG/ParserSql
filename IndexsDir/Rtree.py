from rtree import index
import os
import glob


class RTreeIndex:
    def __init__(self, atribute_index, atribute_type, file_name,
                 is_create_bin=False, data_name=None, records=None, order=10):

        atribute_type = atribute_type.lower()

        if atribute_type not in ["int", "serial", "float", "array"]:
            raise ValueError("El Rtree solo funciona con tipo enteros.")

        self.filename = file_name
        self.atribute_index = atribute_index
        self.atribute_type = atribute_type

        self.dir_store_rtree = os.path.join(data_name, f"{self.atribute_index}")

        os.makedirs(self.dir_store_rtree, exist_ok=True)

        self.idx_file_prefix = os.path.join(self.dir_store_rtree, "rtree_index")

        if not is_create_bin:
            for f in glob.glob(self.idx_file_prefix + '.*'):
                os.remove(f)

        if not is_create_bin:

            self.prop = index.Property()
            self.prop.dat_extension = 'data'
            self.prop.idx_extension = 'index'
            self.prop.leaf_capacity = order
            self.prop.index_capacity = order
            self.prop.near_minimum_overlap_factor = max(1, order // 2)

            self.idx = index.Index(self.idx_file_prefix, properties=self.prop)

            for i, record in enumerate(records):
                attr_val = record.to_dict()[self.atribute_index.lower()]
                bounds = self._get_bounds_from_key(attr_val)
                self.idx.insert(i, bounds)

        else:
            self.idx = index.Index(self.idx_file_prefix)

    def _get_bounds_from_key(self, key):
        if isinstance(key, (list, tuple)) and len(key) in [2, 4]:
            return tuple(map(float, key)) if len(key) == 4 else (key[0], key[1], key[0], key[1])
        elif isinstance(key, (int, float)) or self.atribute_type == "serial":
            return (float(key), float(key), float(key), float(key))
        raise ValueError(f"Clave no válida para RTree: {key}")

    def insert(self, record_id, record):
        attr_val = record[self.atribute_index]
        bounds = self._get_bounds_from_key(attr_val)
        self.idx.insert(record_id, bounds)

    def search(self, key):
        bounds = self._get_bounds_from_key(key)
        return list(self.idx.intersection(bounds))

    def range_search(self, start_key, end_key):
        sx1, sy1, sx2, sy2 = self._get_bounds_from_key(start_key)
        ex1, ey1, ex2, ey2 = self._get_bounds_from_key(end_key)
        bbox = (min(sx1, ex1), min(sy1, ey1), max(sx2, ex2), max(sy2, ey2))
        return list(self.idx.intersection(bbox))

    def delete(self, record_id, key):
        bounds = self._get_bounds_from_key(key)
        self.idx.delete(record_id, bounds)

    def print_index(self):
        print("IDs presentes en el RTree:")
        for item in self.idx.intersection((-1e10, -1e10, 1e10, 1e10)):
            print(f" - ID: {item}")

    def get_all_ids(self):
        return list(self.idx.intersection((-1e10, -1e10, 1e10, 1e10)))

    def knn_search(self, point, k):
        return list(self.idx.nearest(coordinates=point, num_results=k))

    def range_radio(self, point, radio):

        if isinstance(point, (int, float)):
            low = point - radio
            high = point + radio
            return list(self.idx.intersection((low, high)))

        if isinstance(point, (list, tuple)):
            if isinstance(radio, (int, float)):
                radio = [radio] * len(point)
            bounds = []
            for i in range(len(point)):
                bounds.append(point[i] - radio[i])
                bounds.append(point[i] + radio[i])
            return list(self.idx.intersection(tuple(bounds)))

        raise TypeError("El punto debe ser un número, lista o tupla.")