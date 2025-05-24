from .Btree import BtreeIndex
from .Rtree import RtreeIndex
from .Sequential import SequentialIndex
from .AVL import AVLIndex
from .BruteForm import BruteIndex

GetIndex = {
    "BTREE":  BtreeIndex,
    "RTREE": RtreeIndex,
    "SEQ": SequentialIndex,
    "NOTHING": BruteIndex,
    "AVL": AVLIndex
}
