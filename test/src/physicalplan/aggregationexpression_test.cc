#include "physicalplan/aggregationexpression.h"

#include <gtest/gtest.h>

#include <memory>

#include "absl/strings/string_view.h"
#include "test_utils/test_utils.h"

namespace toyquery {
namespace physicalplan { }  // namespace physicalplan
}  // namespace toyquery

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
