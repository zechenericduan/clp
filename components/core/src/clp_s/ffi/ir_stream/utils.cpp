#include "utils.hpp"

#include <array>
#include <iostream>
#include <span>
#include <string_view>

#include <json/single_include/nlohmann/json.hpp>

namespace clp_s::ffi::ir_stream {
namespace {
static constexpr std::uint8_t UTF8_ACCEPT = 0;
static constexpr std::uint8_t UTF8_REJECT = 1;

auto decode(uint8_t& state, std::uint32_t& codep, uint8_t byte) -> uint8_t {
    static constexpr std::array<std::uint8_t, 400> utf8d
            = {{
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  // 00..1F
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  // 20..3F
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  // 40..5F
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  // 60..7F
                    1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,
                    1,   1,   1,   1,   1,   9,   9,   9,   9,   9,   9,
                    9,   9,   9,   9,   9,   9,   9,   9,   9,   9,  // 80..9F
                    7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,
                    7,   7,   7,   7,   7,   7,   7,   7,   7,   7,   7,
                    7,   7,   7,   7,   7,   7,   7,   7,   7,   7,  // A0..BF
                    8,   8,   2,   2,   2,   2,   2,   2,   2,   2,   2,
                    2,   2,   2,   2,   2,   2,   2,   2,   2,   2,   2,
                    2,   2,   2,   2,   2,   2,   2,   2,   2,   2,  // C0..DF
                    0xA, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3,
                    0x3, 0x3, 0x4, 0x3, 0x3,  // E0..EF
                    0xB, 0x6, 0x6, 0x6, 0x5, 0x8, 0x8, 0x8, 0x8, 0x8, 0x8,
                    0x8, 0x8, 0x8, 0x8, 0x8,  // F0..FF
                    0x0, 0x1, 0x2, 0x3, 0x5, 0x8, 0x7, 0x1, 0x1, 0x1, 0x4,
                    0x6, 0x1, 0x1, 0x1, 0x1,  // s0..s0
                    1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,
                    1,   1,   1,   1,   1,   1,   0,   1,   1,   1,   1,
                    1,   0,   1,   0,   1,   1,   1,   1,   1,   1,  // s1..s2
                    1,   2,   1,   1,   1,   1,   1,   2,   1,   2,   1,
                    1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,
                    1,   2,   1,   1,   1,   1,   1,   1,   1,   1,  // s3..s4
                    1,   2,   1,   1,   1,   1,   1,   1,   1,   2,   1,
                    1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,
                    1,   3,   1,   3,   1,   1,   1,   1,   1,   1,  // s5..s6
                    1,   3,   1,   1,   1,   1,   1,   3,   1,   3,   1,
                    1,   1,   1,   1,   1,   1,   3,   1,   1,   1,   1,
                    1,   1,   1,   1,   1,   1,   1,   1,   1,   1  // s7..s8
            }};

    std::uint8_t const type = utf8d[byte];

    codep = (state != UTF8_ACCEPT) ? (byte & 0x3fu) | (codep << 6u) : (0xFFu >> type) & (byte);

    std::size_t index = 256u + static_cast<size_t>(state) * 16u + static_cast<size_t>(type);
    state = utf8d[index];
    return state;
}

/**
 * Escapes a string and append it to the json string.
 * @param str
 * @param json_str
 */
auto escape_and_append_string_to_json_str(std::string_view str, std::string& json_str) -> bool {
    // TODO: implement our own function to generate escaped string.
    // auto const ref{nlohmann::json(str).dump()};

    json_str.push_back('\"');
    std::uint32_t codepoint{};
    std::uint8_t state{UTF8_ACCEPT};
    for (size_t i{0}; i < str.size(); ++i) {
        auto const byte{static_cast<std::uint8_t>(str[i])};
        switch (decode(state, codepoint, byte)) {
            case UTF8_ACCEPT: {
                switch (codepoint) {
                    case 0x08:  // backspace
                    {
                        json_str.push_back('\\');
                        json_str.push_back('b');
                        break;
                    }

                    case 0x09:  // horizontal tab
                    {
                        json_str.push_back('\\');
                        json_str.push_back('t');
                        break;
                    }

                    case 0x0A:  // newline
                    {
                        json_str.push_back('\\');
                        json_str.push_back('n');
                        break;
                    }

                    case 0x0C:  // formfeed
                    {
                        json_str.push_back('\\');
                        json_str.push_back('f');
                        break;
                    }

                    case 0x0D:  // carriage return
                    {
                        json_str.push_back('\\');
                        json_str.push_back('r');
                        break;
                    }

                    case 0x22:  // quotation mark
                    {
                        json_str.push_back('\\');
                        json_str.push_back('\"');
                        break;
                    }

                    case 0x5C:  // reverse solidus
                    {
                        json_str.push_back('\\');
                        json_str.push_back('\\');
                        break;
                    }

                    default: {
                        // escape control characters (0x00..0x1F) or, if
                        // ensure_ascii parameter is used, non-ASCII characters
                        if ((codepoint <= 0x1F)) {
                            char string_buffer[7]{0};
                            (std::snprintf)(
                                    string_buffer,
                                    7,
                                    "\\u%04x",
                                    static_cast<std::uint16_t>(codepoint)
                            );
                            json_str.append(string_buffer, 0, 6);
                        } else {
                            // copy byte to buffer (all previous bytes
                            // been copied have in default case above)
                            json_str.push_back(str[i]);
                        }
                        break;
                    }
                }
                break;
            }
            case UTF8_REJECT:  // decode found invalid UTF-8 byte
                return false;
            default: {
                json_str.push_back(str[i]);
                break;
            }
        }
    }
    json_str.push_back('\"');
    return true;
}

/**
 * Append a msgpack object to the json str. The object can be a primitive value.
 * @param obj
 * @param json_str
 * @return true on success, false on failure.
 */
[[nodiscard]] auto append_msgpack_obj_to_json_str(msgpack::object const& obj, std::string& json_str)
        -> bool {
    bool retval{true};
    switch (obj.type) {
        case msgpack::type::MAP:
            retval = append_msgpack_map_to_json_str(obj, json_str);
            break;
        case msgpack::type::ARRAY:
            retval = append_msgpack_array_to_json_str(obj, json_str);
            break;
        case msgpack::type::NIL:
            json_str += "null";
            break;
        case msgpack::type::BOOLEAN:
            json_str += obj.as<bool>() ? "true" : "false";
            break;
        case msgpack::type::STR:
            retval = escape_and_append_string_to_json_str(obj.as<std::string_view>(), json_str);
            break;
        case msgpack::type::FLOAT32:
        case msgpack::type::FLOAT:
            json_str += nlohmann::json(obj.as<double>()).dump();
            break;
        case msgpack::type::POSITIVE_INTEGER:
            json_str += std::to_string(obj.as<uint64_t>());
            break;
        case msgpack::type::NEGATIVE_INTEGER:
            json_str += std::to_string(obj.as<int64_t>());
            break;
        default:
            retval = false;
            break;
    }
    return retval;
}
}  // namespace

auto append_msgpack_array_to_json_str(msgpack::object const& array, std::string& json_str) -> bool {
    json_str.push_back('[');
    auto const array_data{array.via.array};
    bool is_first_element{true};
    for (auto const& element : std::span{array_data.ptr, static_cast<size_t>(array_data.size)}) {
        if (is_first_element) {
            is_first_element = false;
        } else {
            json_str.push_back(',');
        }
        if (false == append_msgpack_obj_to_json_str(element, json_str)) {
            return false;
        }
    }
    json_str.push_back(']');
    return true;
}

auto append_msgpack_map_to_json_str(msgpack::object const& map, std::string& json_str) -> bool {
    json_str.push_back('{');
    auto const& map_data{map.via.map};
    bool is_first_element{true};
    for (auto const& [key, val] : std::span{map_data.ptr, static_cast<size_t>(map_data.size)}) {
        if (is_first_element) {
            is_first_element = false;
        } else {
            json_str.push_back(',');
        }
        escape_and_append_string_to_json_str(key.as<std::string_view>(), json_str);
        json_str.push_back(':');
        if (false == append_msgpack_obj_to_json_str(val, json_str)) {
            return false;
        }
    }
    json_str.push_back('}');
    return true;
}
}  // namespace clp_s::ffi::ir_stream
