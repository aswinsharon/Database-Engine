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
    
    LeafNode* leaf = FindLeafNode(key);
    if (leaf == nullptr) {
        return false;
    }
    
    bool success = InsertIntoLeaf(leaf, key, value);
    buffer_pool_manager_->UnpinPage(leaf->next_leaf, success);  // Assuming we stored page_id in next_leaf temporarily
    return success;
}

bool SimpleBTree::Search(int key, RID* result) {
    if (IsEmpty()) {
        return false;
    }
    
    LeafNode* leaf = FindLeafNode(key);
    if (leaf == nullptr) {
        return false;
    }
    
    int index = leaf->FindKey(key);
    bool found = (index >= 0 && index < leaf->num_keys && leaf->keys[index] == key);
    
    if (found) {
        *result = leaf->values[index];
    }
    
    buffer_pool_manager_->UnpinPage(root_page_id_, false);  // Simplified - should track actual page
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
    if (IsEmpty()) {
        return nullptr;
    }
    
    if (is_root_leaf_) {
        return GetLeafNode(root_page_id_);
    }
    
    // For now, assume single level (root is always leaf)
    // TODO: Implement multi-level traversal
    return GetLeafNode(root_page_id_);
}

bool SimpleBTree::InsertIntoLeaf(LeafNode* leaf, int key, const RID& value) {
    if (leaf->IsFull()) {
        // TODO: Implement split logic
        std::cerr << "Leaf node is full - split not implemented yet" << std::endl;
        return false;
    }
    
    return leaf->Insert(key, value);
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