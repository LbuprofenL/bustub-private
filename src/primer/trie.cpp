#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  // is_empty
  if (key.empty()) {
    if (this->root_ == nullptr) {
      return nullptr;
    }
    if (this->root_->is_value_node_) {
      auto result_ptr = this->root_;
      auto result_cast = dynamic_cast<const TrieNodeWithValue<T> *>(result_ptr.get());
      if (result_cast) {
        const T *result = result_cast->value_.get();
        return result;
      }
      return nullptr;  // 或者返回适当的值，或抛出异常
    }
    return nullptr;
  }
  if (this->root_ == nullptr || this->root_->children_.empty()) {
    return nullptr;
  }

  auto cur_node = this->root_;

  for (auto it = key.begin(); it != key.end(); it++) {
    if (cur_node->children_.empty()) {
      return nullptr;
    }
    if (cur_node->children_.find(*it) == cur_node->children_.end()) {
      return nullptr;
    }
    if (it == key.end() - 1) {
      auto result_ptr = cur_node->children_.find(*it)->second;
      auto result_cast = dynamic_cast<const TrieNodeWithValue<T> *>(result_ptr.get());
      if (result_cast) {
        const T *result = result_cast->value_.get();
        return result;
      }
      return nullptr;  // 或者返回适当的值，或抛出异常
    }
    cur_node = cur_node->children_.find(*it)->second;
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  // 0.prepare
  auto value_ptr = std::make_shared<T>(std::move(value));
  std::shared_ptr<const TrieNode> tmp_root;
  // edge case:empty string
  if (key.empty()) {
    auto old_root = this->root_->Clone();
    auto result = TrieNodeWithValue<T>(old_root->children_, value_ptr);
    auto result_ptr = std::make_shared<const TrieNodeWithValue<T>>(result);
    return Trie(result_ptr);
  }

  // 1. create a new Trie
  for (auto it = key.end() - 1; it != key.begin() - 1; it--) {
    std::map<char, std::shared_ptr<const TrieNode>> m;
    if (it + 1 == key.end()) {
      auto new_child = TrieNodeWithValue<T>(value_ptr);
      auto new_child_ptr = std::make_shared<const TrieNodeWithValue<T>>(new_child);
      m[*it] = new_child_ptr;
      auto p = new_child.children_['a'];
    } else {
      auto new_child_ptr = std::make_shared<const TrieNode>(tmp_root->children_);
      m[*it] = new_child_ptr;
    }
    tmp_root = std::make_shared<const TrieNode>(m);
  }

  // 2. compare
  auto old_cur = this->root_;
  // edge case::empty_trie
  if (old_cur == nullptr) {
    return Trie(tmp_root);
  }

  auto new_root_unique = this->root_->Clone();
  auto new_root = std::shared_ptr<TrieNode>(std::move(new_root_unique));
  auto cur_node = new_root;
  for (auto it = key.begin(); it != key.end(); it++) {
    if (cur_node->children_.empty()) {
      tmp_root = tmp_root->children_.find(*it)->second;
      cur_node->children_[*it] = tmp_root;
      break;
    }
    if (cur_node->children_.find(*it) == cur_node->children_.end()) {
      tmp_root = tmp_root->children_.find(*it)->second;
      cur_node->children_[*it] = tmp_root;
      break;
    }

    // clone the matched node
    auto new_child = cur_node->children_.find(*it)->second->Clone();
    if (it == key.end() - 1) {
      // has child and this the last element
      auto leaf_ptr = std::make_shared<const TrieNodeWithValue<T>>(new_child->children_, value_ptr);
      cur_node->children_[*it] = leaf_ptr;
      break;
    }
    // update cur_node
    auto next_cur = std::shared_ptr<TrieNode>(std::move(new_child));
    std::shared_ptr<const TrieNode> pp(next_cur);
    cur_node->children_[*it] = pp;
    cur_node = next_cur;

    // update tmp_root
    tmp_root = tmp_root->children_.find(*it)->second;
  }

  // 3. return
  std::shared_ptr<const TrieNode> result = new_root;
  return Trie(result);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value anymore,
  // you should convert it to `TrieNode`. If a node doesn't have children anymore, you should remove it.

  // 0.prepare
  auto old_root = this->root_->Clone();
  // edge case:empty string
  if (key.empty()) {
    auto result = std::make_shared<const TrieNode>(old_root->children_);
    return Trie(result);
  }

  // 1. compare
  // assuming the root node won't be empty
  auto new_root_unique = this->root_->Clone();
  auto new_root = std::shared_ptr<TrieNode>(std::move(new_root_unique));
  auto cur_node = new_root;
  auto tmp_leaf = new_root;
  bool is_empty_trie = true;
  for (auto it = key.begin(); it != key.end(); it++) {
    if (cur_node->children_.empty()) {
      break;
    }
    if (cur_node->children_.find(*it) == cur_node->children_.end()) {
      break;
    }
    // update the potential leaf node
    if (cur_node->children_.size() == 1 && cur_node->is_value_node_) {
      tmp_leaf = cur_node;
    }
    if (cur_node->children_.size() != 1) {
      tmp_leaf = new_root;
    }
    if (cur_node->children_.size() != 1 || cur_node->is_value_node_) {
      is_empty_trie = false;
    }

    if (it == key.end() - 1) {
      // hit
      auto hit_node = cur_node->children_.find(*it)->second->Clone();
      if (hit_node->children_.empty()) {
        // no child
        cur_node->children_.erase(*it);
        if (tmp_leaf != new_root) {
          tmp_leaf->children_ = std::map<char, std::shared_ptr<const TrieNode>>();
        }
        if (is_empty_trie) {
          new_root = nullptr;
        }
      } else {
        // only remove value
        cur_node->children_[*it] = std::make_shared<const TrieNode>(hit_node->children_);
      }

      // return new trie
      std::shared_ptr<const TrieNode> result = new_root;
      return Trie(result);
    }

    // clone the matched node
    auto new_child = cur_node->children_.find(*it)->second->Clone();

    // update cur_node
    auto next_cur = std::shared_ptr<TrieNode>(std::move(new_child));
    std::shared_ptr<const TrieNode> pp(next_cur);
    cur_node->children_[*it] = pp;
    cur_node = next_cur;
  }

  // 2. return
  return *this;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
