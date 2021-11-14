set(sources
  src/toyquery.cc
)

set(exe_sources
		src/main.cc
		${sources}
)

set(headers
    include/common/iterator.h
    include/common/macros.h
    include/datasource/datasource.h
    include/datasource/record_batch_iterator.h
    include/toyquery.h
)

set(test_sources
  src/toyquery_test.cc
)
