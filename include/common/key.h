#ifndef COMMON_KEY_H
#define COMMON_KEY_H

#include <memory>
#include <vector>

#include "arrow/api.h"

namespace toyquery {

/**
 * @brief The key of arrow::ScalerVector i.e. vector of arrow::Scaler
 *
 */
struct Key {
  Key(arrow::ScalarVector scalars) : scalars_{ std::move(scalars) } { }

  bool operator==(const Key& other) const {
    if (scalars_.size() != other.scalars_.size()) return false;

    for (int idx = 0; idx < scalars_.size(); idx++) {
      if (!scalars_[idx]->Equals(other.scalars_[idx])) { return false; }
    }

    return true;
  }

  arrow::ScalarVector scalars_;
};

}  // namespace toyquery

namespace std {

// Implement std::hash for toyquery::Key so that it can used in std::unordered_map as the key. For now, just do the simple
// XOR.
template<>
struct hash<toyquery::Key> {
  std::size_t operator()(const toyquery::Key& k) const {
    std::size_t ret = 0;
    for (auto& i : k.scalars_) { ret ^= i->hash(); }
    return ret;
  }
};

}  // namespace std

#endif  // COMMON_KEY_H