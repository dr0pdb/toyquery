set(sources
  src/common/status.cc
  src/dataframe/dataframe.cc
  src/datasource/datasource.cc
  src/execution/execution_context.cc
  src/logicalplan/logicalplan.cc
  src/physicalplan/accumulator.cc
  src/physicalplan/physicalexpression.cc
  src/physicalplan/physicalplan.cc
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
    include/common/macros.h
    include/common/status.h
    include/dataframe/dataframe.h
    include/datasource/datasource.h
    include/execution/execution_context.h
    include/physicalplan/accumulator.h
    include/physicalplan/aggregationexpression.h
    include/physicalplan/physicalexpression.h
    include/physicalplan/physicalplan.h
    include/logicalplan/logicalexpression.h
    include/logicalplan/logicalplan.h
    include/toyquery.h
)

set(test_sources
  src/toyquery_test.cc
)
