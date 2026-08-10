#pragma once

#include <string>
#include <exception>

#ifndef EP_STATIC_ASSERT
#define EP_STATIC_ASSERT(cond, reason) static_assert(cond, reason)
#endif

class EPException : public std::exception {
   private:
    std::string message = {};

   public:
    explicit EPException(const char* name, const char* file, const int line,
                         const std::string& error) {
        message = std::string("Failed: ") + name + " error " + file + ":" +
                  std::to_string(line) + " '" + error + "'";
    }

    const char* what() const noexcept override { return message.c_str(); }
};

#ifndef HIP_CHECK
#define HIP_CHECK(cmd)                                   \
    do {                                                 \
        hipError_t e = (cmd);                            \
        if (e != hipSuccess) {                           \
            throw EPException("HIP", __FILE__, __LINE__, \
                              hipGetErrorString(e));     \
        }                                                \
    } while (0)
#endif

#ifndef EP_HOST_ASSERT
#define EP_HOST_ASSERT(cond)                                           \
    do {                                                               \
        if (not(cond)) {                                               \
            throw EPException("Assertion", __FILE__, __LINE__, #cond); \
        }                                                              \
    } while (0)
#endif

#ifndef EP_DEVICE_ASSERT
#define EP_DEVICE_ASSERT(cond)                                           \
    do {                                                                 \
        if (not(cond)) {                                                 \
            printf("Assertion failed: %s:%d, condition: %s\n", __FILE__, \
                   __LINE__, #cond);                                     \
            __builtin_trap();                                            \
        }                                                                \
    } while (0)
#endif
