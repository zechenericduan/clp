#include <chrono>
#include <fstream>
#include <iostream>
#include <msgpack.hpp>

#include <Catch2/single_include/catch2/catch.hpp>
#include <json/single_include/nlohmann/json.hpp>

#include "../src/clp/FileWriter.hpp"
#include "../src/clp_s/ffi/ir_stream/serialization_methods.hpp"
#include "../src/clp_s/ffi/ir_stream/SerializationBuffer.hpp"
#include "../src/clp_s/ffi/ir_stream/utils.hpp"
#include "../src/clp_s/ffi/SchemaTree.hpp"
#include "../src/clp_s/ffi/SchemaTreeNode.hpp"

using clp_s::ffi::ir_stream::append_msgpack_array_to_json_str;
using clp_s::ffi::ir_stream::append_msgpack_map_to_json_str;
using clp_s::ffi::ir_stream::SerializationBuffer;
using clp_s::ffi::ir_stream::serialize_end_of_stream;
using clp_s::ffi::ir_stream::serialize_key_value_pair_record;
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
               "This is \"escaped\" string\n",
               nullptr,
               {{"key0", "This is a key value pair record"},
                {"key1", "Key value pair record again, lol"}},
               {-1,
                -0.11111,
                false,
                "This is a string",
                nullptr,
                {{"key0", "This is a key value pair record"},
                 {"key2\"escaped", "This is \"escaped\"\n"},
                 {"inner_key0", {{"inner_key1", "inner"}, {"inner_key2", {{"inner_key3", -4}}}}},
                 {"key2", {1, 0.11111, false, nullptr}}}}};
    auto const msgpack_data{nlohmann::json::to_msgpack(json_array)};
    msgpack::object_handle oh;
    msgpack::unpack(oh, reinterpret_cast<char const*>(msgpack_data.data()), msgpack_data.size());

    std::string json_array_str;
    REQUIRE(append_msgpack_array_to_json_str(oh.get(), json_array_str));
    auto const converted_json_array = nlohmann::json::parse(json_array_str);
    REQUIRE(converted_json_array == json_array);
}

TEST_CASE("structured_ir_encoding", "[ffi][structured]") {
    std::string const prefix{"/Users/lzh/clp_ir_old/clp/components/core/build/"};
    std::string const file_path{prefix + "data/cisco.json"};
    std::string const output_path{file_path + ".msgpack.clp"};
    std::ifstream fin;

    fin.open(file_path);
    std::string line;
    clp::FileWriter writer;
    writer.open(output_path, clp::FileWriter::OpenMode::CREATE_FOR_WRITING);
    SerializationBuffer buffer;
    long long microseconds{0};
    while (getline(fin, line)) {
        nlohmann::json item = nlohmann::json::parse(line);
        auto const data{nlohmann::json::to_msgpack(item)};
        msgpack::object_handle oh;
        msgpack::unpack(oh, reinterpret_cast<char const*>(data.data()), data.size());

        auto const start{std::chrono::high_resolution_clock::now()};
        REQUIRE(serialize_key_value_pair_record(oh.get(), buffer));
        auto const end{std::chrono::high_resolution_clock::now()};
        auto const elapsed{std::chrono::duration_cast<std::chrono::microseconds>(end - start)};
        microseconds += elapsed.count();

        auto const ir_buf{buffer.get_ir_buf()};
        writer.write(ir_buf.data(), ir_buf.size());
        buffer.flush_ir_buf();
    }
    serialize_end_of_stream(buffer);
    std::cerr << "Total time: " << static_cast<double>(microseconds) / 1000000.0 << "\n";
    auto const ir_buf{buffer.get_ir_buf()};
    writer.write(ir_buf.data(), ir_buf.size());
    buffer.flush_ir_buf();
    writer.close();
    fin.close();
}
