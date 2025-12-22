#pragma once

#include "common/config.h"
#include "common/types.h"
#include "buffer/buffer_pool_manager.h"
#include <vector>
#include <memory>

namespace minidb {

/**
 * Simple B+ Tree implementation for integer keys and RID values.
 * This is a concrete implementation to avoid template complexity.
 */
class SimpleBTree {
public:
    /**
     * Constructor
     */
    explicit SimpleBTree(BufferPoolManager* buffer_pool_manager);
    
    /**
     * Destructor
     */
    ~SimpleBTree() = default;

    // Non-copyable
    SimpleBTree(const SimpleBTree&) = delete;
    SimpleBTree& operator=(const SimpleBTree&) = delete;

    /**
     * Insert a key-value pair
     */
    bool Insert(int key, const RID& value);

    /**
     * Search for a key
     */
    bool Search(int key, RID* result);

    /**
     * Remove a key
     */
    bool Remove(int key);

    /**
     * Check if tree is empty
     */
    bool IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

    /**
     * Print tree structure (for debugging)
     */
    void PrintTree();

private:
    struct LeafNode {
        static constexpr int MAX_KEYS = 100;  // Simplified for now
        
        int num_keys;
        int keys[MAX_KEYS];
        RID values[MAX_KEYS];
        page_id_t next_leaf;
        
        LeafNode() : num_keys(0), next_leaf(INVALID_PAGE_ID) {}
        
        int FindKey(int key);
        bool Insert(int key, const RID& value);
        bool Remove(int key);
        bool IsFull() const { return num_keys >= MAX_KEYS; }
    };

    struct InternalNode {
        static constexpr int MAX_KEYS = 100;  // Simplified for now
        
        int num_keys;
        int keys[MAX_KEYS];
        page_id_t children[MAX_KEYS + 1];
        
        InternalNode() : num_keys(0) {}
        
        page_id_t FindChild(int key);
        bool Insert(int key, page_id_t child);
        bool IsFull() const { return num_keys >= MAX_KEYS; }
    };

    // Helper methods
    LeafNode* GetLeafNode(page_id_t page_id);
    InternalNode* GetInternalNode(page_id_t page_id);
    
    page_id_t CreateLeafPage();
    page_id_t CreateInternalPage();
    
    LeafNode* FindLeafNode(int key);
    bool InsertIntoLeaf(LeafNode* leaf, int key, const RID& value);
    
    BufferPoolManager* buffer_pool_manager_;
    page_id_t root_page_id_;
    bool is_root_leaf_;  // True if root is a leaf node
};

}  // namespace minidb