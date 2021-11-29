set(sources
  src/dataframe/dataframe.cc
  src/execution/execution_context.cc
  src/logicalplan/logicalplan.cc
  src/physicalplan/physicalexpression.cc
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
    include/dataframe/dataframe.h
    include/datasource/datasource.h
    include/datasource/record_batch_iterator.h
    include/execution/execution_context.h
    include/physicalplan/physicalexpression.h
    include/physicalplan/physicalplan.h
    include/logicalplan/logicalexpression.h
    include/logicalplan/logicalplan.h
    include/toyquery.h
)

set(test_sources
  src/toyquery_test.cc
)
