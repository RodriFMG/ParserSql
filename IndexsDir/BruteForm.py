import os


class BruteIndex:

    def __index__(self, table_name, attribute_name, list_record):

        self.table = table_name
        self.attribute = attribute_name

        self.bin_path_table_index = f"./bin_data/BRUTE/{self.table}/"
        self.bin_path_index = os.path.join(self.bin_path_table_index, "{self.attribute}.bin")
        self.list_record = list_record

        if not os.path.exists(self.bin_path_table_index):
            os.makedirs(self.bin_path_table_index)

        if not os.path.exists(self.bin_path_index):
            # Escribir para crear un binario vacio.
            with open(self.bin_path_index, "wb") as f:
                pass

    #def insert(self, record):

