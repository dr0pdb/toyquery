#include "toyquery.h"

#include <gtest/gtest.h>

TEST(ToyqueryAddTest, CheckValues) {
  ASSERT_EQ(toyquery::add(1, 2), 3);
  EXPECT_TRUE(true);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
