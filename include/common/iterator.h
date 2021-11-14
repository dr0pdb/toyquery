#ifndef COMMON_ITERATOR_H
#define COMMON_ITERATOR_H

#include "common/macros.h"

namespace toyquery {
namespace common {

template<typename Item>
class Iterator {
 public:
  Iterator() = default;
  ~Iterator() = default;

  /**
   * @brief Get the next item from the iterator.
   *
   * @return Item: the next item.
   */
  virtual Item Next() = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(Iterator);
};

}  // namespace common
}  // namespace toyquery

#endif  // COMMON_ITERATOR_H