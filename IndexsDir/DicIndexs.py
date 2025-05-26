from .Btree import BTreeIndex
from .Rtree import RTreeIndex
from .Sequential import SequentialIndex
from .AVL import AVLIndex
from .ISAM import ISAMIndex
from .Hash import HashIndex

GetIndex = {
    "BTREE":  BTreeIndex,
    "RTREE": RTreeIndex,
    "SEQ": SequentialIndex,
    "AVL": AVLIndex,
    "ISAM": ISAMIndex,
    "HASH": HashIndex
}