#include "index/simple_btree.h"
#include <iostream>
#include <algorithm>
#include <cstring>

namespace minidb {

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

SimpleBTree::LeafNode* SimpleBTree::GetLeafNode(page_id_t page_id) {
    Page* page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
        return nullptr;
    }
    
    return reinterpret_cast<LeafNode*>(page->GetDataArea());
}

SimpleBTree::InternalNode* SimpleBTree::GetInternalNode(page_id_t page_id) {
    Page* page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
        return nullptr;
    }
    
    return reinterpret_cast<InternalNode*>(page->GetDataArea());
}

page_id_t SimpleBTree::CreateLeafPage() {
    page_id_t new_page_id;
    Page* page = buffer_pool_manager_->NewPage(&new_page_id);
    
    if (page == nullptr) {
        return INVALID_PAGE_ID;
    }
    
    // Initialize leaf node
    LeafNode* leaf = reinterpret_cast<LeafNode*>(page->GetDataArea());
    new (leaf) LeafNode();  // Placement new to call constructor
    
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return new_page_id;
}

page_id_t SimpleBTree::CreateInternalPage() {
    page_id_t new_page_id;
    Page* page = buffer_pool_manager_->NewPage(&new_page_id);
    
    if (page == nullptr) {
        return INVALID_PAGE_ID;
    }
    
    // Initialize internal node
    InternalNode* internal = reinterpret_cast<InternalNode*>(page->GetDataArea());
    new (internal) InternalNode();  // Placement new to call constructor
    
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return new_page_id;
}

SimpleBTree::LeafNode* SimpleBTree::FindLeafNode(int key) {
    page_id_t leaf_page_id = FindLeafPageId(key);
    return GetLeafNode(leaf_page_id);
}

page_id_t SimpleBTree::FindLeafPageId(int key) {
    if (IsEmpty()) {
        return INVALID_PAGE_ID;
    }
    
    page_id_t current_page_id = root_page_id_;
    
    while (!is_root_leaf_ || current_page_id != root_page_id_) {
        // Check if current page is a leaf
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (page == nullptr) {
            return INVALID_PAGE_ID;
        }
        
        // For simplicity, assume we can determine node type
        // In a real implementation, you'd store node type in page header
        if (current_page_id == root_page_id_ && is_root_leaf_) {
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            return current_page_id;
        }
        
        // Treat as internal node
        InternalNode* internal = reinterpret_cast<InternalNode*>(page->GetDataArea());
        page_id_t child_page_id = internal->FindChild(key);
        buffer_pool_manager_->UnpinPage(current_page_id, false);
        
        current_page_id = child_page_id;
        
        // Check if we've reached a leaf (simplified check)
        Page* child_page = buffer_pool_manager_->FetchPage(child_page_id);
        if (child_page == nullptr) {
            return INVALID_PAGE_ID;
        }
        
        // For now, assume all non-root pages at depth 1 are leaves
        // This is a simplification - in practice you'd track node types
        buffer_pool_manager_->UnpinPage(child_page_id, false);
        return child_page_id;
    }
    
    return root_page_id_;
}

bool SimpleBTree::InsertIntoLeaf(LeafNode* leaf, int key, const RID& value) {
    if (leaf->IsFull()) {
        return false; // Should be handled by split logic
    }
    
    return leaf->Insert(key, value);
}

void SimpleBTree::SplitLeafNode(LeafNode* leaf, page_id_t leaf_page_id) {
    // Create new leaf node
    page_id_t new_leaf_page_id = CreateLeafPage();
    LeafNode* new_leaf = GetLeafNode(new_leaf_page_id);
    
    // Calculate split point
    int split_point = LeafNode::MAX_KEYS / 2;
    
    // Move half the keys to new leaf
    for (int i = split_point; i < leaf->num_keys; ++i) {
        new_leaf->keys[i - split_point] = leaf->keys[i];
        new_leaf->values[i - split_point] = leaf->values[i];
    }
    new_leaf->num_keys = leaf->num_keys - split_point;
    leaf->num_keys = split_point;
    
    // Update leaf pointers
    new_leaf->next_leaf = leaf->next_leaf;
    leaf->next_leaf = new_leaf_page_id;
    new_leaf->parent = leaf->parent;
    
    // Get the key to promote to parent
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

void SimpleBTree::SplitInternalNode(InternalNode* internal, page_id_t internal_page_id) {
    // Create new internal node
    page_id_t new_internal_page_id = CreateInternalPage();
    InternalNode* new_internal = GetInternalNode(new_internal_page_id);
    
    // Calculate split point
    int split_point = InternalNode::MAX_KEYS / 2;
    int promote_key = internal->keys[split_point];
    
    // Move keys and children to new internal node
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

void SimpleBTree::InsertIntoParent(page_id_t left_page_id, int key, page_id_t right_page_id) {
    // Get parent page
    LeafNode* left_leaf = GetLeafNode(left_page_id);
    page_id_t parent_page_id = left_leaf->parent;
    buffer_pool_manager_->UnpinPage(left_page_id, false);
    
    InternalNode* parent = GetInternalNode(parent_page_id);
    
    if (!parent->IsFull()) {
        // Insert directly into parent
        parent->Insert(key, right_page_id);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
    } else {
        // Parent is full, need to split
        SplitInternalNode(parent, parent_page_id);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
        
        // Retry insertion
        InsertIntoParent(left_page_id, key, right_page_id);
    }
}

void SimpleBTree::CreateNewRoot(page_id_t left_page_id, int key, page_id_t right_page_id) {
    // Create new root as internal node
    page_id_t new_root_page_id = CreateInternalPage();
    InternalNode* new_root = GetInternalNode(new_root_page_id);
    
    // Set up new root
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
    is_root_leaf_ = false;
    
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
}

void SimpleBTree::PrintTree() {
    if (IsEmpty()) {
        std::cout << "Empty tree" << std::endl;
        return;
    }
    
    LeafNode* root = GetLeafNode(root_page_id_);
    std::cout << "Root (leaf): ";
    for (int i = 0; i < root->num_keys; ++i) {
        std::cout << root->keys[i] << " ";
    }
    std::cout << std::endl;
    
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
}

// LeafNode methods
int SimpleBTree::LeafNode::FindKey(int key) {
    // Binary search
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
    
    return left;  // Insert position
}

bool SimpleBTree::LeafNode::Insert(int key, const RID& value) {
    if (IsFull()) {
        return false;
    }
    
    int pos = FindKey(key);
    
    // Check if key already exists
    if (pos < num_keys && keys[pos] == key) {
        return false;  // Duplicate key
    }
    
    // Shift elements to make room
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
page_id_t SimpleBTree::InternalNode::FindChild(int key) {
    int i = 0;
    while (i < num_keys && key >= keys[i]) {
        i++;
    }
    return children[i];
}

bool SimpleBTree::InternalNode::Insert(int key, page_id_t child) {
    if (IsFull()) {
        return false;
    }
    
    int pos = 0;
    while (pos < num_keys && key > keys[pos]) {
        pos++;
    }
    
    // Shift elements
    for (int i = num_keys; i > pos; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
    }
    
    keys[pos] = key;
    children[pos + 1] = child;
    num_keys++;
    
    return true;
}

}  // namespace minidb