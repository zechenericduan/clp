#include <fstream>
#include <iostream>
#include <msgpack.hpp>

#include <Catch2/single_include/catch2/catch.hpp>
#include <json/single_include/nlohmann/json.hpp>

#include "../src/clp_s/ffi/ir_stream/SerializationBuffer.hpp"
#include "../src/clp_s/ffi/ir_stream/utils.hpp"
#include "../src/clp_s/ffi/SchemaTree.hpp"
#include "../src/clp_s/ffi/SchemaTreeNode.hpp"

using clp_s::ffi::ir_stream::append_msgpack_array_to_json_str;
using clp_s::ffi::ir_stream::append_msgpack_map_to_json_str;
using clp_s::ffi::SchemaTree;
using clp_s::ffi::SchemaTreeNode;

TEST_CASE("schema_tree", "[ffi][structured]") {
    SchemaTree schema_tree;

    auto test_node = [&schema_tree](
                             SchemaTreeNode::id_t parent_id,
                             std::string_view key_name,
                             SchemaTreeNode::Type type,
                             SchemaTreeNode::id_t expected_id,
                             bool already_exist
                     ) -> void {
        SchemaTreeNode::id_t node_id{};
        SchemaTree::TreeNodeLocator locator{parent_id, key_name, type};
        if (already_exist) {
            REQUIRE(schema_tree.has_node(locator, node_id));
            REQUIRE(expected_id == node_id);
        } else {
            REQUIRE(false == schema_tree.has_node(locator, node_id));
            REQUIRE(expected_id == schema_tree.insert_node(locator));
        }
    };

    test_node(SchemaTree::cRootId, "a", SchemaTreeNode::Type::Obj, 1, false);
    test_node(SchemaTree::cRootId, "a", SchemaTreeNode::Type::Int, 2, false);
    test_node(1, "b", SchemaTreeNode::Type::Obj, 3, false);
    test_node(3, "c", SchemaTreeNode::Type::Obj, 4, false);
    schema_tree.take_snapshot();
    test_node(3, "d", SchemaTreeNode::Type::Int, 5, false);
    test_node(3, "d", SchemaTreeNode::Type::Bool, 6, false);
    test_node(4, "d", SchemaTreeNode::Type::Array, 7, false);
    test_node(4, "d", SchemaTreeNode::Type::Str, 8, false);

    test_node(SchemaTree::cRootId, "a", SchemaTreeNode::Type::Obj, 1, true);
    test_node(SchemaTree::cRootId, "a", SchemaTreeNode::Type::Int, 2, true);
    test_node(1, "b", SchemaTreeNode::Type::Obj, 3, true);
    test_node(3, "c", SchemaTreeNode::Type::Obj, 4, true);
    test_node(3, "d", SchemaTreeNode::Type::Int, 5, true);
    test_node(3, "d", SchemaTreeNode::Type::Bool, 6, true);
    test_node(4, "d", SchemaTreeNode::Type::Array, 7, true);
    test_node(4, "d", SchemaTreeNode::Type::Str, 8, true);

    schema_tree.revert();
    test_node(SchemaTree::cRootId, "a", SchemaTreeNode::Type::Obj, 1, true);
    test_node(SchemaTree::cRootId, "a", SchemaTreeNode::Type::Int, 2, true);
    test_node(1, "b", SchemaTreeNode::Type::Obj, 3, true);
    test_node(3, "c", SchemaTreeNode::Type::Obj, 4, true);
    test_node(3, "d", SchemaTreeNode::Type::Int, 5, false);
    test_node(3, "d", SchemaTreeNode::Type::Bool, 6, false);
    test_node(4, "d", SchemaTreeNode::Type::Array, 7, false);
    test_node(4, "d", SchemaTreeNode::Type::Str, 8, false);

    test_node(SchemaTree::cRootId, "a", SchemaTreeNode::Type::Obj, 1, true);
    test_node(SchemaTree::cRootId, "a", SchemaTreeNode::Type::Int, 2, true);
    test_node(1, "b", SchemaTreeNode::Type::Obj, 3, true);
    test_node(3, "c", SchemaTreeNode::Type::Obj, 4, true);
    test_node(3, "d", SchemaTreeNode::Type::Int, 5, true);
    test_node(3, "d", SchemaTreeNode::Type::Bool, 6, true);
    test_node(4, "d", SchemaTreeNode::Type::Array, 7, true);
    test_node(4, "d", SchemaTreeNode::Type::Str, 8, true);
}

TEST_CASE("append_json_str", "[ffi][structured]") {
    nlohmann::json const json_array
            = {1,
               0.11111,
               false,
               "This is a string",
               nullptr,
               {{"key0", "This is a key value pair record"},
                {"key1", "Key value pair record again, lol"}},
               {-1,
                -0.11111,
                false,
                "This is a string",
                nullptr,
                {{"key0", "This is a key value pair record"},
                 {"inner_key0", {{"inner_key1", "inner"}, {"inner_key2", {{"inner_key3", -4}}}}},
                 {"key1", {1, 0.11111, false, nullptr}}}}};
    auto const msgpack_data{nlohmann::json::to_msgpack(json_array)};
    msgpack::object_handle oh;
    msgpack::unpack(oh, reinterpret_cast<char const*>(msgpack_data.data()), msgpack_data.size());

    std::string json_array_str;
    REQUIRE(append_msgpack_array_to_json_str(oh.get(), json_array_str));
    auto const converted_json_array = nlohmann::json::parse(json_array_str);
    REQUIRE(converted_json_array == json_array);
}

void traverse(msgpack::object const& obj) {
    switch (obj.type) {
        case msgpack::type::MAP: {
            msgpack::object_map map = obj.via.map;
            for (uint32_t i = 0; i < map.size; ++i) {
                msgpack::object_kv kv = map.ptr[i];
                std::string key;
                kv.key.convert(key);  // Assuming keys are strings
                std::cout << "Key: " << key << std::endl;
                traverse(kv.val);  // Recursively traverse the value
            }
            break;
        }
        case msgpack::type::ARRAY: {
            msgpack::object_array array = obj.via.array;
            for (uint32_t i = 0; i < array.size; ++i) {
                traverse(array.ptr[i]);  // Recursively traverse each item
            }
            break;
        }
        case msgpack::type::NIL:
            std::cout << "Null" << std::endl;
            break;
        case msgpack::type::BOOLEAN:
            std::cout << "Boolean: " << obj.as<bool>() << std::endl;
            break;
        case msgpack::type::POSITIVE_INTEGER:
            std::cout << "Positive Integer: " << obj.as<uint64_t>() << std::endl;
            break;
        case msgpack::type::NEGATIVE_INTEGER:
            std::cout << "Negative Integer: " << obj.as<int64_t>() << std::endl;
            break;
        case msgpack::type::FLOAT:
            std::cout << "Float: " << obj.as<double>() << std::endl;
            break;
        case msgpack::type::STR:
            std::cout << "String: " << obj.as<std::string>() << std::endl;
            break;
        // Add cases for other types as needed (e.g., binary, ext)
        default:
            std::cout << "Unhandled type" << std::endl;
    }
}

TEST_CASE("msgpack", "[ffi][structured]") {
    std::string const file_path{"msgpack/test.json"};
    std::ifstream f{file_path};
    nlohmann::json data{nlohmann::json::parse(f)};
    auto const msgpack_data{nlohmann::json::to_msgpack(data)};
    msgpack::object_handle oh;
    msgpack::unpack(oh, reinterpret_cast<char const*>(msgpack_data.data()), msgpack_data.size());
    traverse(oh.get());
}

// TEST_CASE("json_transform", "[ffi][structured]") {

//     nlohmann::json data = nlohmann::json::parse(f);
//     for (auto const& item : data) {

//         std::cerr << item.msgpack().dump() << "\n";
//     }
// }
