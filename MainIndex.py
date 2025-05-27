from IndexsDir.DicIndexs import GetIndex
import os
import sys
import numpy as np


def usar_bin(connection):
    from bin_data.BinaryManager import BinStorageManager
    return BinStorageManager(pg_conn=connection)


# En caso no soporte ese indice las operaciones, pasamos con su siguiente indice anclado.
class MainIndex:

    def __init__(self, table_name, atributte_name, typeIndex, conn):

        self.table = table_name.lower()
        self.attribute = atributte_name.lower()

        typeIndex = typeIndex or "AVL"

        self.typeIndex = typeIndex.upper()
        self.bin_manager = usar_bin(conn)

        self.bin_path_table_index = f"./bin_data/{self.typeIndex}/{self.table}/"
        self.bin_path_index = os.path.join(self.bin_path_table_index, f"{self.attribute}.bin")

        if not os.path.exists(self.bin_path_table_index):
            os.makedirs(self.bin_path_table_index)

        self.attribute_type = self.bin_manager.get_type_att(self.table, self.attribute)

        # size del keyhandler para las estructuras de datos.
        records_table = self.bin_manager.load_records_as_objects(table_name)

        if records_table:
            size_keyhandler = np.max([
                sys.getsizeof(record.to_dict()[self.attribute])
                for record
                in records_table])
        else:
            # Un valor de default, por el momento no se está insertando.
            size_keyhandler = 20

        print("Max size a utilizar: ", size_keyhandler)

        if not os.path.exists(self.bin_path_index):

            if self.typeIndex not in ["RTREE", "HASH"]:
                with open(self.bin_path_index, "wb") as f:
                    pass

            self.Index = GetIndex[self.typeIndex](
                self.attribute,
                self.attribute_type,
                self.bin_path_index,
                records=records_table,
                is_create_bin=False,
                data_name=self.bin_path_table_index,
                size_kh=size_keyhandler
            )

        else:
            self.Index = GetIndex[self.typeIndex](
                self.attribute,
                self.attribute_type,
                self.bin_path_index,
                is_create_bin=True,
                data_name=self.bin_path_table_index,
                size_kh=size_keyhandler
            )

    def insert(self, key, record):
        self.Index.insert_record(key, record)

    def search(self, idx):
        return self.Index.search(idx)

    def range_search(self, idx1, idx2):

        if self.typeIndex == "HASH":
            print(f"El indice {self.typeIndex} no soporta RangeQuery\n")
            return None

        return self.Index.range_search(idx1, idx2)

    def delete(self, idx1):
        self.Index.delete(idx1)

    def kNN(self, point, k):

        if self.typeIndex != "RTREE":
            print(f"El indice {self.typeIndex} no soporta kNN\n")
            return None

        return self.Index.knn_search(point, k)

    def range_radio(self, point, r):

        if self.typeIndex != "RTREE":
            print(f"El indice {self.typeIndex} no soporta busqueda por radio\n")
            return None

        return self.Index.range_radio(point, r)
