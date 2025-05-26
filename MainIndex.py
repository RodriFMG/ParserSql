from IndexsDir.DicIndexs import GetIndex
import os
from bin_data.BinaryManager import BinStorageManager


# En caso no soporte ese indice las operaciones, pasamos con su siguiente indice anclado.
class MainIndex:

    def __init__(self, table_name, atributte_name, typeIndex, conn):

        self.table = table_name.lower()
        self.attribute = atributte_name
        self.typeIndex = typeIndex.upper()
        self.bin_manager = BinStorageManager(pg_conn=conn)


        self.bin_path_table_index = f"./bin_data/{self.typeIndex}/{self.table}/"
        self.bin_path_index = os.path.join(self.bin_path_table_index, f"{self.attribute}.bin")

        if not os.path.exists(self.bin_path_table_index):
            os.makedirs(self.bin_path_table_index)

        self.attribute_type = self.bin_manager.get_type_att(self.table, self.attribute)

        if not os.path.exists(self.bin_path_index):
            records_table = self.bin_manager.load_records_as_objects(table_name)

            if typeIndex != "RTREE":
                with open(self.bin_path_index, "wb") as f:
                    pass


            self.Index = GetIndex[typeIndex](self.attribute, self.attribute_type, self.bin_path_index,
                                             records=records_table,
                                             is_create_bin=False,
                                             data_name = self.bin_path_table_index)

        else:
            self.Index = GetIndex[typeIndex](self.attribute, self.attribute_type, self.bin_path_index,
                                             is_create_bin=True,
                                             data_name = self.bin_path_table_index)

    def insert(self, key, record):
        self.Index.insert(key, record)

    def search(self, idx):
        return self.Index.search(idx)

    def range_search(self, idx1, idx2):

        if self.typeIndex == "HASH":
            print(f"El indice {self.typeIndex} no soporta RangeQuery")

        return self.Index.range_search(idx1, idx2)

    def delete(self, idx1):
        self.Index.delete(idx1)

    def kNN(self, point, k):

        if self.typeIndex != "RTREE":
            print(f"El indice {self.typeIndex} no soporta kNN")

        return self.Index.knn_search(point, k)

    def range_radio(self, point, r):

        if self.typeIndex != "RTREE":
            print(f"El indice {self.typeIndex} no soporta busqueda por radio")

        return self.Index.range_radio(point, r)