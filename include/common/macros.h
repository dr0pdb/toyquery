#ifndef COMMON_MACROS_H
#define COMMON_MACROS_H

namespace toyquery {

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

}  // namespace toyquery

#endif  // COMMON_MACROS_H