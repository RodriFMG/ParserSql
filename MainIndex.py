from IndexsDir.DicIndexs import GetIndex
import os


class MainIndex:

    def __init__(self, table_name, atributte_name, typeIndex):
        self.table = table_name
        self.attribute = atributte_name
        self.typeIndex = typeIndex

        self.bin_path_table_index = f"./bin_data/{typeIndex}/{self.table}/"
        self.bin_path_index = os.path.join(self.bin_path_table_index, "{self.attribute}.bin")

        if not os.path.exists(self.bin_path_table_index):
            os.makedirs(self.bin_path_table_index)

        if not os.path.exists(self.bin_path_index):
            # Escribir para crear un binario vacio.
            with open(self.bin_path_index, "wb") as f:
                pass

        # Creando el indice...
        self.Index = GetIndex[typeIndex](...)

    def insert(self, record):
        self.Index.insert(record)

    def search(self, idx):
        return self.Index.search(idx)

    def range_search(self, idx1, idx2):
        return self.Index.range_search(idx1, idx2)

    def delete(self, idx1):
        self.Index.delete(idx1)

    def kNN(self, r, k):
        if self.typeIndex != "RTREE":
            raise ValueError(f"El indice {self.typeIndex} no soporta kNN")

        return self.Index.kNN(r, k)
