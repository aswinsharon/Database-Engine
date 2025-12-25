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
        static constexpr int MAX_KEYS = 10;  // Smaller for testing splits
        
        int num_keys;
        int keys[MAX_KEYS];
        RID values[MAX_KEYS];
        page_id_t next_leaf;
        page_id_t parent;
        
        LeafNode() : num_keys(0), next_leaf(INVALID_PAGE_ID), parent(INVALID_PAGE_ID) {}
        
        int FindKey(int key);
        bool Insert(int key, const RID& value);
        bool Remove(int key);
        bool IsFull() const { return num_keys >= MAX_KEYS; }
        bool IsHalfFull() const { return num_keys >= MAX_KEYS / 2; }
        
        // Split operations
        LeafNode* Split(page_id_t new_page_id);
        int GetMiddleKey() const { return keys[MAX_KEYS / 2]; }
    };

    struct InternalNode {
        static constexpr int MAX_KEYS = 10;  // Smaller for testing splits
        
        int num_keys;
        int keys[MAX_KEYS];
        page_id_t children[MAX_KEYS + 1];
        page_id_t parent;
        
        InternalNode() : num_keys(0), parent(INVALID_PAGE_ID) {}
        
        page_id_t FindChild(int key);
        bool Insert(int key, page_id_t child);
        bool IsFull() const { return num_keys >= MAX_KEYS; }
        bool IsHalfFull() const { return num_keys >= MAX_KEYS / 2; }
        
        // Split operations
        InternalNode* Split(page_id_t new_page_id);
        int GetMiddleKey() const { return keys[MAX_KEYS / 2]; }
    };

    // Helper methods
    LeafNode* GetLeafNode(page_id_t page_id);
    InternalNode* GetInternalNode(page_id_t page_id);
    
    page_id_t CreateLeafPage();
    page_id_t CreateInternalPage();
    
    LeafNode* FindLeafNode(int key);
    bool InsertIntoLeaf(LeafNode* leaf, int key, const RID& value);
    
    // Split and merge operations
    void SplitLeafNode(LeafNode* leaf, page_id_t leaf_page_id);
    void SplitInternalNode(InternalNode* internal, page_id_t internal_page_id);
    void InsertIntoParent(page_id_t left_page_id, int key, page_id_t right_page_id);
    void CreateNewRoot(page_id_t left_page_id, int key, page_id_t right_page_id);
    
    // Tree traversal
    page_id_t FindLeafPageId(int key);
    
    BufferPoolManager* buffer_pool_manager_;
    page_id_t root_page_id_;
    bool is_root_leaf_;  // True if root is a leaf node
};

}  // namespace minidb