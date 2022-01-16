set(sources
  src/common/status.cc
  src/dataframe/dataframe.cc
  src/datasource/datasource.cc
  src/execution/execution_context.cc
  src/logicalplan/logicalplan.cc
  src/optimization/optimizer.cc
  src/optimization/utils.cc
  src/physicalplan/accumulator.cc
  src/physicalplan/physicalexpression.cc
  src/physicalplan/physicalplan.cc
  src/planner/planner.cc
  src/sql/expressions.cc
  src/sql/parser.cc
  src/sql/planner.cc
  src/sql/tokenizer.cc
  src/toyquery.cc
)

set(exe_sources
		src/main.cc
		${sources}
)

set(headers
    include/common/arrow.h
    include/common/debug.h
    include/common/iterator.h
    include/common/key.h
    include/common/macros.h
    include/common/status.h
    include/common/utils.h
    include/dataframe/dataframe.h
    include/datasource/datasource.h
    include/execution/execution_context.h
    include/optimization/optimizer.h
    include/optimization/utils.h
    include/physicalplan/accumulator.h
    include/physicalplan/aggregationexpression.h
    include/physicalplan/physicalexpression.h
    include/physicalplan/physicalplan.h
    include/planner/planner.h
    include/logicalplan/logicalexpression.h
    include/logicalplan/logicalplan.h
    include/logicalplan/utils.h
    include/sql/expressions.h
    include/sql/parser.h
    include/sql/planner.h
    include/sql/tokenizer.h
    include/sql/tokens.h
    include/test_utils/test_utils.h
    include/toyquery.h
)

set(test_sources
  src/datasource/datasource_test.cc
  src/physicalplan/accumulator_test.cc
  src/toyquery_test.cc
)
