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
     * Insert a key-value pair into the B+ tree
     * @param key The integer key to insert
     * @param value The RID value associated with the key
     * @return true if insertion was successful, false if key already exists or error occurred
     */
    bool Insert(int key, const RID& value);

    /**
     * Search for a key in the B+ tree
     * @param key The integer key to search for
     * @param result Pointer to store the found RID value
     * @return true if key was found, false otherwise
     */
    bool Search(int key, RID* result);

    /**
     * Remove a key from the B+ tree
     * @param key The integer key to remove
     * @return true if removal was successful, false if key not found
     */
    bool Remove(int key);

    /**
     * Check if the B+ tree is empty
     * @return true if tree has no elements, false otherwise
     */
    bool IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

    /**
     * Print the tree structure for debugging purposes
     * Displays all keys in the tree in a readable format
     */
    void PrintTree();

private:
    struct LeafNode {
        static constexpr int MAX_KEYS = 10;  // Smaller for testing splits
        
        int num_keys;                    ///< Number of keys currently stored
        int keys[MAX_KEYS];             ///< Array of keys in sorted order
        RID values[MAX_KEYS];           ///< Array of RID values corresponding to keys
        page_id_t next_leaf;            ///< Page ID of next leaf node (for range scans)
        page_id_t parent;               ///< Page ID of parent internal node
        
        LeafNode() : num_keys(0), next_leaf(INVALID_PAGE_ID), parent(INVALID_PAGE_ID) {}
        
        /**
         * Find the position of a key using binary search
         * @param key The key to search for
         * @return Index where key is found, or insertion position if not found
         */
        int FindKey(int key);
        
        /**
         * Insert a key-value pair into this leaf node
         * @param key The key to insert
         * @param value The RID value to associate with the key
         * @return true if insertion successful, false if node is full or key exists
         */
        bool Insert(int key, const RID& value);
        
        /**
         * Remove a key from this leaf node
         * @param key The key to remove
         * @return true if removal successful, false if key not found
         */
        bool Remove(int key);
        
        /**
         * Check if the leaf node is at maximum capacity
         * @return true if node cannot accept more keys
         */
        bool IsFull() const { return num_keys >= MAX_KEYS; }
        
        /**
         * Check if the leaf node is at least half full
         * @return true if node has at least half the maximum keys
         */
        bool IsHalfFull() const { return num_keys >= MAX_KEYS / 2; }
        
        /**
         * Split this leaf node, moving half the keys to a new node
         * @param new_page_id The page ID of the new leaf node to split into
         * @return Pointer to the new leaf node
         */
        LeafNode* Split(page_id_t new_page_id);
        
        /**
         * Get the middle key for splitting purposes
         * @return The key at the middle position
         */
        int GetMiddleKey() const { return keys[MAX_KEYS / 2]; }
    };

    struct InternalNode {
        static constexpr int MAX_KEYS = 10;  // Smaller for testing splits
        
        int num_keys;                        ///< Number of keys currently stored
        int keys[MAX_KEYS];                 ///< Array of separator keys in sorted order
        page_id_t children[MAX_KEYS + 1];   ///< Array of child page IDs (one more than keys)
        page_id_t parent;                   ///< Page ID of parent internal node
        
        InternalNode() : num_keys(0), parent(INVALID_PAGE_ID) {}
        
        /**
         * Find the appropriate child page for a given key
         * @param key The key to search for
         * @return Page ID of the child that should contain the key
         */
        page_id_t FindChild(int key);
        
        /**
         * Insert a key and corresponding child pointer into this internal node
         * @param key The separator key to insert
         * @param child The page ID of the right child for this key
         * @return true if insertion successful, false if node is full
         */
        bool Insert(int key, page_id_t child);
        
        /**
         * Check if the internal node is at maximum capacity
         * @return true if node cannot accept more keys
         */
        bool IsFull() const { return num_keys >= MAX_KEYS; }
        
        /**
         * Check if the internal node is at least half full
         * @return true if node has at least half the maximum keys
         */
        bool IsHalfFull() const { return num_keys >= MAX_KEYS / 2; }
        
        /**
         * Split this internal node, moving half the keys to a new node
         * @param new_page_id The page ID of the new internal node to split into
         * @return Pointer to the new internal node
         */
        InternalNode* Split(page_id_t new_page_id);
        
        /**
         * Get the middle key for splitting purposes
         * @return The key at the middle position
         */
        int GetMiddleKey() const { return keys[MAX_KEYS / 2]; }
    };

    // Helper methods
    /**
     * Get a leaf node from the buffer pool manager
     * @param page_id The page ID of the leaf node
     * @return Pointer to the leaf node, or nullptr if page cannot be fetched
     */
    LeafNode* GetLeafNode(page_id_t page_id);
    
    /**
     * Get an internal node from the buffer pool manager
     * @param page_id The page ID of the internal node
     * @return Pointer to the internal node, or nullptr if page cannot be fetched
     */
    InternalNode* GetInternalNode(page_id_t page_id);
    
    /**
     * Create a new leaf page and initialize it
     * @return Page ID of the newly created leaf page, or INVALID_PAGE_ID on failure
     */
    page_id_t CreateLeafPage();
    
    /**
     * Create a new internal page and initialize it
     * @return Page ID of the newly created internal page, or INVALID_PAGE_ID on failure
     */
    page_id_t CreateInternalPage();
    
    /**
     * Find the leaf node that should contain a given key
     * @param key The key to search for
     * @return Pointer to the appropriate leaf node, or nullptr on error
     */
    LeafNode* FindLeafNode(int key);
    
    /**
     * Insert a key-value pair into a specific leaf node
     * @param leaf Pointer to the leaf node
     * @param key The key to insert
     * @param value The RID value to associate with the key
     * @return true if insertion successful, false if node is full
     */
    bool InsertIntoLeaf(LeafNode* leaf, int key, const RID& value);
    
    // Split and merge operations
    /**
     * Split a full leaf node into two nodes
     * @param leaf Pointer to the leaf node to split
     * @param leaf_page_id Page ID of the leaf node being split
     */
    void SplitLeafNode(LeafNode* leaf, page_id_t leaf_page_id);
    
    /**
     * Split a full internal node into two nodes
     * @param internal Pointer to the internal node to split
     * @param internal_page_id Page ID of the internal node being split
     */
    void SplitInternalNode(InternalNode* internal, page_id_t internal_page_id);
    
    /**
     * Insert a key and right child pointer into the parent of two nodes
     * @param left_page_id Page ID of the left child
     * @param key The separator key between left and right children
     * @param right_page_id Page ID of the right child
     */
    void InsertIntoParent(page_id_t left_page_id, int key, page_id_t right_page_id);
    
    /**
     * Create a new root node when the current root splits
     * @param left_page_id Page ID of the left child of new root
     * @param key The separator key for the new root
     * @param right_page_id Page ID of the right child of new root
     */
    void CreateNewRoot(page_id_t left_page_id, int key, page_id_t right_page_id);
    
    // Tree traversal
    /**
     * Find the page ID of the leaf that should contain a given key
     * @param key The key to search for
     * @return Page ID of the appropriate leaf page, or INVALID_PAGE_ID on error
     */
    page_id_t FindLeafPageId(int key);
    
    BufferPoolManager* buffer_pool_manager_;  ///< Buffer pool for page management
    page_id_t root_page_id_;                  ///< Page ID of the root node
    bool is_root_leaf_;                       ///< True if root is a leaf node (single level tree)
};

}  // namespace minidb