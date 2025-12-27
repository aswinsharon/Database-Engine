#include "index/simple_btree.h"
#include <iostream>
#include <algorithm>
#include <cstring>

namespace minidb {

/**
 * Constructor for SimpleBTree
 * @param buffer_pool_manager Pointer to the buffer pool manager for page operations
 */
SimpleBTree::SimpleBTree(BufferPoolManager* buffer_pool_manager)
    : buffer_pool_manager_(buffer_pool_manager), 
      root_page_id_(INVALID_PAGE_ID),
      is_root_leaf_(true) {}

bool SimpleBTree::Insert(int key, const RID& value) {
    if (IsEmpty()) {
        // Create root as leaf node
        root_page_id_ = CreateLeafPage();
        is_root_leaf_ = true;
        
        LeafNode* root = GetLeafNode(root_page_id_);
        bool success = root->Insert(key, value);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        return success;
    }
    
    page_id_t leaf_page_id = FindLeafPageId(key);
    LeafNode* leaf = GetLeafNode(leaf_page_id);
    if (leaf == nullptr) {
        return false;
    }
    
    // Check if key already exists
    int pos = leaf->FindKey(key);
    if (pos < leaf->num_keys && leaf->keys[pos] == key) {
        buffer_pool_manager_->UnpinPage(leaf_page_id, false);
        return false; // Duplicate key
    }
    
    // If leaf is not full, insert directly
    if (!leaf->IsFull()) {
        bool success = leaf->Insert(key, value);
        buffer_pool_manager_->UnpinPage(leaf_page_id, success);
        return success;
    }
    
    // Leaf is full, need to split
    SplitLeafNode(leaf, leaf_page_id);
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    
    // Retry insertion after split
    return Insert(key, value);
}

bool SimpleBTree::Search(int key, RID* result) {
    if (IsEmpty()) {
        return false;
    }
    
    page_id_t leaf_page_id = FindLeafPageId(key);
    LeafNode* leaf = GetLeafNode(leaf_page_id);
    if (leaf == nullptr) {
        return false;
    }
    
    int index = leaf->FindKey(key);
    bool found = (index >= 0 && index < leaf->num_keys && leaf->keys[index] == key);
    
    if (found) {
        *result = leaf->values[index];
    }
    
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return found;
}

/**
 * Remove a key from the B+ tree
 * Currently implements basic removal without rebalancing
 */
bool SimpleBTree::Remove(int key) {
    if (IsEmpty()) {
        return false;
    }
    
    LeafNode* leaf = FindLeafNode(key);
    if (leaf == nullptr) {
        return false;
    }
    
    bool success = leaf->Remove(key);
    buffer_pool_manager_->UnpinPage(root_page_id_, success);  // Simplified
    return success;
}

/**
 * Get a leaf node from the buffer pool
 * @param page_id The page ID to fetch
 * @return Pointer to leaf node or nullptr on failure
 */
SimpleBTree::LeafNode* SimpleBTree::GetLeafNode(page_id_t page_id) {
    Page* page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
        return nullptr;
    }
    
    return reinterpret_cast<LeafNode*>(page->GetDataArea());
}

/**
 * Get an internal node from the buffer pool
 * @param page_id The page ID to fetch
 * @return Pointer to internal node or nullptr on failure
 */
SimpleBTree::InternalNode* SimpleBTree::GetInternalNode(page_id_t page_id) {
    Page* page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
        return nullptr;
    }
    
    return reinterpret_cast<InternalNode*>(page->GetDataArea());
}

/**
 * Create a new leaf page and initialize it
 * @return Page ID of new leaf page or INVALID_PAGE_ID on failure
 */
page_id_t SimpleBTree::CreateLeafPage() {
    page_id_t new_page_id;
    Page* page = buffer_pool_manager_->NewPage(&new_page_id);
    
    if (page == nullptr) {
        return INVALID_PAGE_ID;
    }
    
    // Initialize leaf node using placement new
    LeafNode* leaf = reinterpret_cast<LeafNode*>(page->GetDataArea());
    new (leaf) LeafNode();  // Placement new to call constructor
    
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return new_page_id;
}

/**
 * Create a new internal page and initialize it
 * @return Page ID of new internal page or INVALID_PAGE_ID on failure
 */
page_id_t SimpleBTree::CreateInternalPage() {
    page_id_t new_page_id;
    Page* page = buffer_pool_manager_->NewPage(&new_page_id);
    
    if (page == nullptr) {
        return INVALID_PAGE_ID;
    }
    
    // Initialize internal node using placement new
    InternalNode* internal = reinterpret_cast<InternalNode*>(page->GetDataArea());
    new (internal) InternalNode();  // Placement new to call constructor
    
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return new_page_id;
}

/**
 * Find the leaf node that should contain a given key
 * @param key The key to search for
 * @return Pointer to the appropriate leaf node
 */
SimpleBTree::LeafNode* SimpleBTree::FindLeafNode(int key) {
    page_id_t leaf_page_id = FindLeafPageId(key);
    return GetLeafNode(leaf_page_id);
}

/**
 * Find the page ID of the leaf that should contain a given key
 * Traverses the tree from root to leaf following the B+ tree structure
 * @param key The key to search for
 * @return Page ID of the appropriate leaf page
 */
page_id_t SimpleBTree::FindLeafPageId(int key) {
    if (IsEmpty()) {
        return INVALID_PAGE_ID;
    }
    
    page_id_t current_page_id = root_page_id_;
    
    // If root is a leaf, return it directly
    if (is_root_leaf_) {
        return current_page_id;
    }
    
    // Traverse internal nodes until we reach a leaf
    while (true) {
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (page == nullptr) {
            return INVALID_PAGE_ID;
        }
        
        // Treat as internal node and find appropriate child
        InternalNode* internal = reinterpret_cast<InternalNode*>(page->GetDataArea());
        page_id_t child_page_id = internal->FindChild(key);
        buffer_pool_manager_->UnpinPage(current_page_id, false);
        
        // For simplicity, assume all children of internal nodes are leaves
        // In a full implementation, you'd track node types or tree depth
        return child_page_id;
    }
}

/**
 * Insert a key-value pair into a specific leaf node
 * @param leaf Pointer to the leaf node
 * @param key The key to insert
 * @param value The RID value to associate with the key
 * @return true if insertion successful, false if node is full
 */
bool SimpleBTree::InsertIntoLeaf(LeafNode* leaf, int key, const RID& value) {
    if (leaf->IsFull()) {
        return false; // Should be handled by split logic
    }
    
    return leaf->Insert(key, value);
}

/**
 * Split a full leaf node into two nodes
 * Moves half the keys to a new leaf and updates parent pointers
 * @param leaf Pointer to the leaf node to split
 * @param leaf_page_id Page ID of the leaf node being split
 */
void SimpleBTree::SplitLeafNode(LeafNode* leaf, page_id_t leaf_page_id) {
    // Create new leaf node
    page_id_t new_leaf_page_id = CreateLeafPage();
    LeafNode* new_leaf = GetLeafNode(new_leaf_page_id);
    
    // Calculate split point (move upper half to new leaf)
    int split_point = LeafNode::MAX_KEYS / 2;
    
    // Move half the keys to new leaf
    for (int i = split_point; i < leaf->num_keys; ++i) {
        new_leaf->keys[i - split_point] = leaf->keys[i];
        new_leaf->values[i - split_point] = leaf->values[i];
    }
    new_leaf->num_keys = leaf->num_keys - split_point;
    leaf->num_keys = split_point;
    
    // Update leaf pointers for linked list traversal
    new_leaf->next_leaf = leaf->next_leaf;
    leaf->next_leaf = new_leaf_page_id;
    new_leaf->parent = leaf->parent;
    
    // Get the key to promote to parent (first key of new leaf)
    int promote_key = new_leaf->keys[0];
    
    buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);
    
    // Insert into parent or create new root
    if (leaf->parent == INVALID_PAGE_ID) {
        // This is root, create new root
        CreateNewRoot(leaf_page_id, promote_key, new_leaf_page_id);
    } else {
        InsertIntoParent(leaf_page_id, promote_key, new_leaf_page_id);
    }
}

/**
 * Split a full internal node into two nodes
 * Moves half the keys and children to a new internal node
 * @param internal Pointer to the internal node to split
 * @param internal_page_id Page ID of the internal node being split
 */
void SimpleBTree::SplitInternalNode(InternalNode* internal, page_id_t internal_page_id) {
    // Create new internal node
    page_id_t new_internal_page_id = CreateInternalPage();
    InternalNode* new_internal = GetInternalNode(new_internal_page_id);
    
    // Calculate split point and get the key to promote
    int split_point = InternalNode::MAX_KEYS / 2;
    int promote_key = internal->keys[split_point];
    
    // Move keys and children to new internal node (skip the promoted key)
    for (int i = split_point + 1; i < internal->num_keys; ++i) {
        new_internal->keys[i - split_point - 1] = internal->keys[i];
    }
    for (int i = split_point + 1; i <= internal->num_keys; ++i) {
        new_internal->children[i - split_point - 1] = internal->children[i];
    }
    
    new_internal->num_keys = internal->num_keys - split_point - 1;
    internal->num_keys = split_point;
    new_internal->parent = internal->parent;
    
    buffer_pool_manager_->UnpinPage(new_internal_page_id, true);
    
    // Insert into parent or create new root
    if (internal->parent == INVALID_PAGE_ID) {
        CreateNewRoot(internal_page_id, promote_key, new_internal_page_id);
    } else {
        InsertIntoParent(internal_page_id, promote_key, new_internal_page_id);
    }
}

/**
 * Insert a key and right child pointer into the parent of two nodes
 * Handles the case where parent might also need to split
 * @param left_page_id Page ID of the left child
 * @param key The separator key between left and right children
 * @param right_page_id Page ID of the right child
 */
void SimpleBTree::InsertIntoParent(page_id_t left_page_id, int key, page_id_t right_page_id) {
    // Get parent page ID from left child
    LeafNode* left_leaf = GetLeafNode(left_page_id);
    page_id_t parent_page_id = left_leaf->parent;
    buffer_pool_manager_->UnpinPage(left_page_id, false);
    
    InternalNode* parent = GetInternalNode(parent_page_id);
    
    if (!parent->IsFull()) {
        // Insert directly into parent
        parent->Insert(key, right_page_id);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
    } else {
        // Parent is full, need to split it first
        SplitInternalNode(parent, parent_page_id);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
        
        // Retry insertion after split
        InsertIntoParent(left_page_id, key, right_page_id);
    }
}

/**
 * Create a new root node when the current root splits
 * This increases the height of the tree by one level
 * @param left_page_id Page ID of the left child of new root
 * @param key The separator key for the new root
 * @param right_page_id Page ID of the right child of new root
 */
void SimpleBTree::CreateNewRoot(page_id_t left_page_id, int key, page_id_t right_page_id) {
    // Create new root as internal node
    page_id_t new_root_page_id = CreateInternalPage();
    InternalNode* new_root = GetInternalNode(new_root_page_id);
    
    // Set up new root with one key and two children
    new_root->keys[0] = key;
    new_root->children[0] = left_page_id;
    new_root->children[1] = right_page_id;
    new_root->num_keys = 1;
    
    // Update child parent pointers
    LeafNode* left_child = GetLeafNode(left_page_id);
    left_child->parent = new_root_page_id;
    buffer_pool_manager_->UnpinPage(left_page_id, true);
    
    LeafNode* right_child = GetLeafNode(right_page_id);
    right_child->parent = new_root_page_id;
    buffer_pool_manager_->UnpinPage(right_page_id, true);
    
    // Update tree metadata
    root_page_id_ = new_root_page_id;
    is_root_leaf_ = false;  // Root is now an internal node
    
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
}

/**
 * Print the tree structure for debugging purposes
 * Shows all keys in the tree in a readable format
 */
void SimpleBTree::PrintTree() {
    if (IsEmpty()) {
        std::cout << "Empty tree" << std::endl;
        return;
    }
    
    if (is_root_leaf_) {
        LeafNode* root = GetLeafNode(root_page_id_);
        std::cout << "Root (leaf): ";
        for (int i = 0; i < root->num_keys; ++i) {
            std::cout << root->keys[i] << " ";
        }
        std::cout << std::endl;
        buffer_pool_manager_->UnpinPage(root_page_id_, false);
    } else {
        std::cout << "Multi-level tree (simplified print)" << std::endl;
        // For a full implementation, you'd traverse and print all levels
    }
}

// LeafNode methods
/**
 * Find the position of a key using binary search
 * @param key The key to search for
 * @return Index where key is found, or insertion position if not found
 */
int SimpleBTree::LeafNode::FindKey(int key) {
    // Binary search for the key position
    int left = 0, right = num_keys - 1;
    
    while (left <= right) {
        int mid = (left + right) / 2;
        if (keys[mid] == key) {
            return mid;
        } else if (keys[mid] < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    
    return left;  // Return insertion position if key not found
}

/**
 * Insert a key-value pair into this leaf node
 * Maintains sorted order of keys
 * @param key The key to insert
 * @param value The RID value to associate with the key
 * @return true if insertion successful, false if node is full or key exists
 */
bool SimpleBTree::LeafNode::Insert(int key, const RID& value) {
    if (IsFull()) {
        return false;
    }
    
    int pos = FindKey(key);
    
    // Check if key already exists
    if (pos < num_keys && keys[pos] == key) {
        return false;  // Duplicate key not allowed
    }
    
    // Shift elements to make room for new key-value pair
    for (int i = num_keys; i > pos; --i) {
        keys[i] = keys[i - 1];
        values[i] = values[i - 1];
    }
    
    // Insert new key-value pair
    keys[pos] = key;
    values[pos] = value;
    num_keys++;
    
    return true;
}

/**
 * Remove a key from this leaf node
 * Maintains sorted order after removal
 * @param key The key to remove
 * @return true if removal successful, false if key not found
 */
bool SimpleBTree::LeafNode::Remove(int key) {
    int pos = FindKey(key);
    
    // Check if key exists
    if (pos >= num_keys || keys[pos] != key) {
        return false;  // Key not found
    }
    
    // Shift elements to fill the gap
    for (int i = pos; i < num_keys - 1; ++i) {
        keys[i] = keys[i + 1];
        values[i] = values[i + 1];
    }
    
    num_keys--;
    return true;
}

// InternalNode methods
/**
 * Find the appropriate child page for a given key
 * Uses the separator keys to determine which child should contain the key
 * @param key The key to search for
 * @return Page ID of the child that should contain the key
 */
page_id_t SimpleBTree::InternalNode::FindChild(int key) {
    int i = 0;
    // Find the first key greater than the search key
    while (i < num_keys && key >= keys[i]) {
        i++;
    }
    return children[i];
}

/**
 * Insert a key and corresponding child pointer into this internal node
 * Maintains sorted order of keys and proper child relationships
 * @param key The separator key to insert
 * @param child The page ID of the right child for this key
 * @return true if insertion successful, false if node is full
 */
bool SimpleBTree::InternalNode::Insert(int key, page_id_t child) {
    if (IsFull()) {
        return false;
    }
    
    // Find insertion position
    int pos = 0;
    while (pos < num_keys && key > keys[pos]) {
        pos++;
    }
    
    // Shift elements to make room
    for (int i = num_keys; i > pos; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
    }
    
    // Insert new key and child pointer
    keys[pos] = key;
    children[pos + 1] = child;
    num_keys++;
    
    return true;
}

}  // namespace minidb