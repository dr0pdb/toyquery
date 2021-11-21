#ifndef DATAFRAME_DATAFRAME_H
#define DATAFRAME_DATAFRAME_H

#include "arrow/api.h"
#include "common/macros.h"

namespace toyquery {
namespace dataframe {

/**
 * @brief An interface to easily create logical plans.
 *
 */
class DataFrame {
 public:
  DataFrame() = default;
  ~DataFrame() = default;

 private:
};

}  // namespace dataframe
}  // namespace toyquery

#endif  // DATAFRAME_DATAFRAME_H