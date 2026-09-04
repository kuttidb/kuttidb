#ifndef KUTTIDB_ADMIN_JSON_H
#define KUTTIDB_ADMIN_JSON_H

#include <stddef.h>
#include <stdint.h>

/* Deliberately small bounded JSON helpers for the Management API.  They do
 * not allocate while locating a field; callers copy only validated values.
 * This is not a general JSON implementation. */
typedef struct AdminJsonSlice {
    const char *data;
    size_t len;
} AdminJsonSlice;

/* Validate one JSON value with a maximum nesting depth of 32. */
int admin_json_validate(const char *json, size_t len);
/* Extract a top-level string, unsigned integer, or boolean field. */
int admin_json_string(const char *json, size_t len, const char *field,
                      AdminJsonSlice *out);
int admin_json_u64(const char *json, size_t len, const char *field,
                   uint64_t *out);
/* Extract a top-level signed integer without accepting floating-point or
 * exponent notation. */
int admin_json_i64(const char *json, size_t len, const char *field,
                   int64_t *out);
int admin_json_bool(const char *json, size_t len, const char *field, int *out);
/* Extract a bounded top-level array of exact (unescaped) JSON strings. */
int admin_json_string_array(const char *json, size_t len, const char *field,
                            AdminJsonSlice *out, size_t cap, size_t *out_count);
/* Extract a bounded top-level array of complete JSON objects. The slices
 * retain their braces and refer to the validated request buffer. */
int admin_json_object_array(const char *json, size_t len, const char *field,
                            AdminJsonSlice *out, size_t cap, size_t *out_count);
/* Decode canonical padded Base64 or canonical unpadded Base64URL. */
int admin_base64_decode(const char *data, size_t len, int urlsafe,
                        unsigned char *out, size_t out_cap, size_t *out_len);

#endif
