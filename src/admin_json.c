#include "admin_json.h"

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define ADMIN_JSON_DEPTH_MAX 32u

/* JSON text is Unicode text.  Validate raw source bytes before parsing so a
 * literal string can never carry malformed UTF-8 into a response or engine
 * identifier. Escapes remain ASCII here and are separately syntax-checked. */
static int valid_utf8(const char *data, size_t len) {
    const unsigned char *p = (const unsigned char *)data;
    for (size_t i = 0; i < len;) {
        unsigned char c = p[i++];
        if (c < 0x80) continue;
        if (c >= 0xc2 && c <= 0xdf) {
            if (i >= len || (p[i++] & 0xc0) != 0x80) return 0;
        } else if (c == 0xe0) {
            if (i + 1 >= len || p[i] < 0xa0 || p[i] > 0xbf || (p[i + 1] & 0xc0) != 0x80) return 0;
            i += 2;
        } else if (c >= 0xe1 && c <= 0xec) {
            if (i + 1 >= len || (p[i] & 0xc0) != 0x80 || (p[i + 1] & 0xc0) != 0x80) return 0;
            i += 2;
        } else if (c == 0xed) {
            if (i + 1 >= len || p[i] < 0x80 || p[i] > 0x9f || (p[i + 1] & 0xc0) != 0x80) return 0;
            i += 2;
        } else if (c >= 0xee && c <= 0xef) {
            if (i + 1 >= len || (p[i] & 0xc0) != 0x80 || (p[i + 1] & 0xc0) != 0x80) return 0;
            i += 2;
        } else if (c == 0xf0) {
            if (i + 2 >= len || p[i] < 0x90 || p[i] > 0xbf || (p[i + 1] & 0xc0) != 0x80 || (p[i + 2] & 0xc0) != 0x80) return 0;
            i += 3;
        } else if (c >= 0xf1 && c <= 0xf3) {
            if (i + 2 >= len || (p[i] & 0xc0) != 0x80 || (p[i + 1] & 0xc0) != 0x80 || (p[i + 2] & 0xc0) != 0x80) return 0;
            i += 3;
        } else if (c == 0xf4) {
            if (i + 2 >= len || p[i] < 0x80 || p[i] > 0x8f || (p[i + 1] & 0xc0) != 0x80 || (p[i + 2] & 0xc0) != 0x80) return 0;
            i += 3;
        } else return 0;
    }
    return 1;
}

static const char *skip_ws(const char *p, const char *end) {
    while (p < end && (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t')) p++;
    return p;
}

static int scan_string(const char **inout, const char *end, AdminJsonSlice *out) {
    const char *p = *inout;
    if (p >= end || *p++ != '"') return -1;
    const char *start = p;
    int escaped = 0;
    while (p < end) {
        unsigned char c = (unsigned char)*p++;
        if (c < 0x20) return -1;
        if (escaped) {
            if (c != '"' && c != '\\' && c != '/' && c != 'b' && c != 'f' &&
                c != 'n' && c != 'r' && c != 't' && c != 'u') return -1;
            if (c == 'u') { for (unsigned i = 0; i < 4; i++) if (p >= end || !isxdigit((unsigned char)*p++)) return -1; }
            escaped = 0;
        } else if (c == '\\') escaped = 1;
        else if (c == '"') {
            if (out) { out->data = start; out->len = (size_t)((p - 1) - start); }
            *inout = p;
            return 0;
        }
    }
    return -1;
}

static int scan_value(const char **inout, const char *end, unsigned depth) {
    const char *p = skip_ws(*inout, end);
    if (depth > ADMIN_JSON_DEPTH_MAX || p >= end) return -1;
    if (*p == '"') { if (scan_string(&p, end, NULL)) return -1; }
    else if (*p == '{' || *p == '[') {
        char open = *p++, close = open == '{' ? '}' : ']';
        p = skip_ws(p, end);
        if (p < end && *p == close) p++;
        else for (;;) {
            if (open == '{') { if (scan_string(&p, end, NULL)) return -1; p = skip_ws(p, end); if (p >= end || *p++ != ':') return -1; }
            if (scan_value(&p, end, depth + 1)) return -1;
            p = skip_ws(p, end);
            if (p < end && *p == close) { p++; break; }
            if (p >= end || *p++ != ',') return -1;
            p = skip_ws(p, end);
        }
    } else {
        const char *start = p;
        while (p < end && !strchr(" \t\r\n,]}", *p)) p++;
        if (start == p) return -1;
        if ((size_t)(p - start) == 4 && !memcmp(start, "true", 4)) {}
        else if ((size_t)(p - start) == 5 && !memcmp(start, "false", 5)) {}
        else if ((size_t)(p - start) == 4 && !memcmp(start, "null", 4)) {}
        else { char *ep = NULL; errno = 0; (void)strtod(start, &ep); if (errno || ep != p) return -1; }
    }
    *inout = p;
    return 0;
}

int admin_json_validate(const char *json, size_t len) {
    if (!json || !len) return -1;
    if (!valid_utf8(json, len)) return -1;
    const char *p = json, *end = json + len;
    if (scan_value(&p, end, 0)) return -1;
    return skip_ws(p, end) == end ? 0 : -1;
}

static int find_field(const char *json, size_t len, const char *field,
                      const char **value, const char **end_value) {
    const char *p = skip_ws(json, json + len), *end = json + len;
    if (p >= end || *p++ != '{') return -1;
    p = skip_ws(p, end);
    while (p < end && *p != '}') {
        AdminJsonSlice key;
        if (scan_string(&p, end, &key)) return -1;
        p = skip_ws(p, end);
        if (p >= end || *p++ != ':') return -1;
        p = skip_ws(p, end);
        const char *begin = p;
        if (scan_value(&p, end, 1)) return -1;
        if (key.len == strlen(field) && !memcmp(key.data, field, key.len)) { *value = begin; *end_value = p; return 1; }
        p = skip_ws(p, end);
        if (p < end && *p == ',') { p = skip_ws(p + 1, end); continue; }
        if (p < end && *p == '}') break;
        return -1;
    }
    return 0;
}

int admin_json_string(const char *json, size_t len, const char *field, AdminJsonSlice *out) {
    const char *p, *end;
    int rc = find_field(json, len, field, &p, &end);
    if (rc != 1 || scan_string(&p, end, out) || p != end) return rc == 0 ? 0 : -1;
    /* Escapes are rejected at the API boundary so decoded bytes remain exact
     * and bounded without a lossy Unicode transformation. */
    if (memchr(out->data, '\\', out->len)) return -1;
    return 1;
}

int admin_json_u64(const char *json, size_t len, const char *field, uint64_t *out) {
    const char *p, *end;
    int rc = find_field(json, len, field, &p, &end);
    if (rc != 1) return rc;
    char tmp[32], *ep;
    if ((size_t)(end - p) >= sizeof tmp) return -1;
    memcpy(tmp, p, (size_t)(end - p)); tmp[end - p] = 0;
    errno = 0; unsigned long long n = strtoull(tmp, &ep, 10);
    if (errno || *ep || *tmp == '-') return -1;
    *out = (uint64_t)n;
    return 1;
}

int admin_json_i64(const char *json, size_t len, const char *field, int64_t *out) {
    const char *p, *end;
    int rc = find_field(json, len, field, &p, &end);
    if (rc != 1) return rc;
    char tmp[32], *ep;
    if ((size_t)(end - p) >= sizeof tmp) return -1;
    memcpy(tmp, p, (size_t)(end - p)); tmp[end - p] = 0;
    errno = 0; long long n = strtoll(tmp, &ep, 10);
    if (errno || *ep) return -1;
    *out = (int64_t)n;
    return 1;
}

int admin_json_bool(const char *json, size_t len, const char *field, int *out) {
    const char *p, *end;
    int rc = find_field(json, len, field, &p, &end);
    if (rc != 1) return rc;
    if (end - p == 4 && !memcmp(p, "true", 4)) { *out = 1; return 1; }
    if (end - p == 5 && !memcmp(p, "false", 5)) { *out = 0; return 1; }
    return -1;
}

int admin_json_string_array(const char *json, size_t len, const char *field,
                            AdminJsonSlice *out, size_t cap, size_t *out_count) {
    const char *p, *end;
    int rc = find_field(json, len, field, &p, &end);
    if (out_count) *out_count = 0;
    if (rc != 1) return rc;
    p = skip_ws(p, end);
    if (p >= end || *p++ != '[') return -1;
    p = skip_ws(p, end);
    size_t count = 0;
    if (p < end && *p == ']') p++;
    else for (;;) {
        AdminJsonSlice item;
        if (count == cap || scan_string(&p, end, &item) || memchr(item.data, '\\', item.len)) return -1;
        out[count++] = item;
        p = skip_ws(p, end);
        if (p < end && *p == ']') { p++; break; }
        if (p >= end || *p++ != ',') return -1;
        p = skip_ws(p, end);
    }
    if (p != end) return -1;
    if (out_count) *out_count = count;
    return 1;
}

int admin_json_object_array(const char *json, size_t len, const char *field,
                            AdminJsonSlice *out, size_t cap, size_t *out_count) {
    const char *p, *end;
    int rc = find_field(json, len, field, &p, &end);
    if (out_count) *out_count = 0;
    if (rc != 1) return rc;
    p = skip_ws(p, end);
    if (p >= end || *p++ != '[') return -1;
    p = skip_ws(p, end);
    size_t count = 0;
    if (p < end && *p == ']') p++;
    else for (;;) {
        const char *begin = p;
        if (count == cap || p >= end || *p != '{' || scan_value(&p, end, 1)) return -1;
        out[count].data = begin; out[count++].len = (size_t)(p - begin);
        p = skip_ws(p, end);
        if (p < end && *p == ']') { p++; break; }
        if (p >= end || *p++ != ',') return -1;
        p = skip_ws(p, end);
    }
    if (p != end) return -1;
    if (out_count) *out_count = count;
    return 1;
}

static int b64_value(unsigned char c, int urlsafe) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == (urlsafe ? '-' : '+')) return 62;
    if (c == (urlsafe ? '_' : '/')) return 63;
    return -1;
}

int admin_base64_decode(const char *data, size_t len, int urlsafe, unsigned char *out, size_t cap, size_t *out_len) {
    if (!data || !out_len || (!urlsafe && (len % 4))) return -1;
    size_t oi = 0; unsigned bits = 0, acc = 0, pad = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)data[i];
        if (c == '=') { if (urlsafe || ++pad > 2 || (i + 1 < len && data[i + 1] != '=')) return -1; continue; }
        if (pad) return -1;
        int v = b64_value(c, urlsafe); if (v < 0) return -1;
        acc = (acc << 6) | (unsigned)v; bits += 6;
        while (bits >= 8) { bits -= 8; if (oi >= cap) return -1; out[oi++] = (unsigned char)(acc >> bits); }
    }
    if ((!urlsafe && !((pad == 0 && bits == 0) || (pad == 1 && bits == 2) || (pad == 2 && bits == 4))) ||
        (urlsafe && bits && bits != 2 && bits != 4)) return -1;
    *out_len = oi;
    return 0;
}
