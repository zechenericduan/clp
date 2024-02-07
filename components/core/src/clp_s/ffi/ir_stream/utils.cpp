#include "utils.hpp"

#include <string_view>

namespace clp_s::ffi::ir_stream {
namespace {
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
            json_str.push_back('\"');
            json_str += obj.as<std::string_view>();
            json_str.push_back('\"');
            break;
        case msgpack::type::FLOAT:
            json_str += std::to_string(obj.as<double>());
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
    auto const array_size{static_cast<size_t>(array_data.size)};
    for (size_t idx{0}; idx < array_size; ++idx) {
        if (0 != idx) {
            json_str.push_back(',');
        }
        auto const& element{array_data.ptr[idx]};
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
    auto const map_size{static_cast<size_t>(map_data.size)};
    for (size_t idx{0}; idx < map_size; ++idx) {
        if (0 != idx) {
            json_str.push_back(',');
        }
        auto const& key_value_pair{map_data.ptr[idx]};
        json_str.push_back('\"');
        json_str += key_value_pair.key.as<std::string_view>();
        json_str.push_back('\"');
        json_str.push_back(':');
        if (false == append_msgpack_obj_to_json_str(key_value_pair.val, json_str)) {
            return false;
        }
    }
    json_str.push_back('}');
    return true;
}
}  // namespace clp_s::ffi::ir_stream
