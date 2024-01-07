package node

import (
	"unsafe"

	"github.com/kris-gaudel/goqlite/constants"
)

const (
	NODE_INTERNAL = "NODE_INTERNAL"
	NODE_LEAF     = "NODE_LEAF"
)

const (
	NODE_TYPE_SIZE          = unsafe.Sizeof(uint8(0))
	NODE_TYPE_OFFSET        = 0
	IS_ROOT_SIZE            = unsafe.Sizeof(uint8(0))
	IS_ROOT_OFFSET          = NODE_TYPE_SIZE
	PARENT_POINTER_SIZE     = unsafe.Sizeof(uint32(0))
	PARENT_POINTER_OFFSET   = IS_ROOT_OFFSET + IS_ROOT_SIZE
	COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
)

const (
	LEAF_NODE_NUM_CELLS_SIZE   = unsafe.Sizeof(uint32(0))
	LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
	LEAF_NODE_HEADER_SIZE      = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE
)

const (
	LEAF_NODE_KEY_SIZE   = unsafe.Sizeof(uint32(0))
	LEAF_NODE_KEY_OFFSET = 0
	LEAF_NODE_VALUE_SIZE = constants.ROW_SIZE
)
