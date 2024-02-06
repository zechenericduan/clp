#include "serialization_methods.hpp"

#include <vector>

#include "../../Utils.hpp"
#include "byteswap.hpp"
#include "protocol_constants.hpp"

using std::vector;

namespace clp_s::ffi::ir_stream {
namespace {
/**
 * This tag determines whether integers will be serialized using bytes and shorts.
 */
static constexpr bool cEnableShortIntCompression{false};

/**
 * Serializes the given integer into the IR stream.
 * @tparam integer_t The size of this type must be 1, 2, 4 or 8 bytes.
 * @param value
 * @param buf
 */
template <typename integer_t>
auto serialize_integer_generic(integer_t value, vector<int8_t>& buf) -> void {
    integer_t value_big_endian{};
    static_assert(
            sizeof(integer_t) == 1 || sizeof(integer_t) == 2 || sizeof(integer_t) == 4
            || sizeof(integer_t) == 8
    );
    if constexpr (sizeof(value) == 1) {
        value_big_endian = value;
    } else if constexpr (sizeof(value) == 2) {
        value_big_endian = bswap_16(value);
    } else if constexpr (sizeof(value) == 4) {
        value_big_endian = bswap_32(value);
    } else if constexpr (sizeof(value) == 8) {
        value_big_endian = bswap_64(value);
    }
    auto const* data{reinterpret_cast<int8_t*>(&value_big_endian)};
    buf.insert(buf.end(), data, data + sizeof(value));
}

/**
 * Serializes the given int value (signed) into the IR stream.
 * @param value
 * @param buf
 * @return true on success.
 * @return false if the given value exceeds the largest encoding range.
 */
[[nodiscard]] auto serialize_int(int64_t value, vector<int8_t>& buf) -> bool {
    if (cEnableShortIntCompression && INT8_MIN <= value && value <= INT8_MAX) {
        buf.push_back(cProtocol::Tag::ValueInt8);
        serialize_integer_generic(static_cast<int8_t>(value), buf);
    } else if (cEnableShortIntCompression && INT16_MIN <= value && value <= INT16_MAX) {
        buf.push_back(cProtocol::Tag::ValueInt16);
        serialize_integer_generic(static_cast<int16_t>(value), buf);
    } else if (INT32_MIN <= value && value <= INT32_MAX) {
        buf.push_back(cProtocol::Tag::ValueInt32);
        serialize_integer_generic(static_cast<int32_t>(value), buf);
    } else if (INT64_MIN <= value && value <= INT64_MAX) {
        buf.push_back(cProtocol::Tag::ValueInt64);
        serialize_integer_generic(static_cast<int64_t>(value), buf);
    } else {
        // Ideally the control flow will never reach here, but it is added in case we change the
        // input type.
        return false;
    }
    return true;
}

/**
 * Serializes the given double into the IR stream.
 * @param value
 * @param buf
 */
auto serialize_double(double value, vector<int8_t>& buf) -> void {
    buf.push_back(cProtocol::Tag::ValueDouble);
    serialize_integer_generic(bit_cast<uint64_t>(value), buf);
}

/**
 * Serializes the given string into the IR stream, using standard string encoding.
 * @param str
 * @param buf
 * @return true on success.
 * @return false on failure if the string length exceeds the encoding range.
 */
[[nodiscard]] auto serialize_str(std::string_view str, std::vector<int8_t>& buf) -> bool {
    auto const length{str.length()};
    if (length <= UINT8_MAX) {
        buf.push_back(cProtocol::Tag::StandardStrLenByte);
        serialize_integer_generic(static_cast<uint8_t>(length), buf);
    } else if (length <= UINT16_MAX) {
        buf.push_back(cProtocol::Tag::StandardStrLenShort);
        serialize_integer_generic(static_cast<uint16_t>(length), buf);
    } else if (length <= UINT32_MAX) {
        buf.push_back(cProtocol::Tag::StandardStrLenInt);
        serialize_integer_generic(static_cast<uint32_t>(length), buf);
    } else {
        return false;
    }
    buf.insert(buf.cend(), str.cbegin(), str.cend());
    return true;
}

/**
 * Serializes the given string into the IR stream, using CLP string encoding.
 * @param str
 * @param buf
 * @return true on success, false on failure.
 */
[[nodiscard]] auto serialize_clp_str(std::string_view str, std::vector<int8_t>& buf) -> bool {
    // TODO: replace this by using the real CLP string encoding.
    buf.push_back(cProtocol::Tag::ValueStrCLPFourByte);
    return serialize_str(str, buf);
}

/**
 * Serializes the given boolean value into the IR stream.
 * @param value
 * @param buf
 */
auto serialize_bool(bool value, std::vector<int8_t>& buf) -> void {
    auto const tag{value ? cProtocol::Tag::ValueTrue : cProtocol::Tag::ValueFalse};
    buf.push_back(tag);
}

/**
 * Serializes the null value into the IR stream.
 * @param buf
 */
auto serialize_null(std::vector<int8_t>& buf) -> void {
    buf.push_back(cProtocol::Tag::ValueNull);
}

/**
 * Serializes an empty object into the IR stream.
 * @param buf
 */
auto serialize_empty_obj(std::vector<int8_t>& buf) -> void {
    buf.push_back(cProtocol::Tag::ValueEmpty);
}
}  // namespace

auto serialize_key_value_pair_records(msgpack::object const& record, SerializationBuffer& ir_buf)
        -> bool {
    return false;
}
}  // namespace clp_s::ffi::ir_stream
