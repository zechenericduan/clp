#include <chrono>
#include <fstream>
#include <iostream>
#include <msgpack.hpp>

#include <Catch2/single_include/catch2/catch.hpp>
#include <json/single_include/nlohmann/json.hpp>

#include "../../clp/BufferReader.hpp"
#include "../../clp/FileWriter.hpp"
#include "../../clp/streaming_compression/zstd/Compressor.hpp"
#include "../../clp/streaming_compression/zstd/Decompressor.hpp"
#include "../clp/ffi/ir_stream/encoding_methods.hpp"
#include "../ffi/ir_stream/deserialization_methods.hpp"
#include "../ffi/ir_stream/serialization_methods.hpp"
#include "../ffi/ir_stream/SerializationBuffer.hpp"

using clp_s::ffi::ir_stream::deserialize_next_key_value_pair_record;
using clp_s::ffi::ir_stream::deserialize_record_as_json_str;
using clp_s::ffi::ir_stream::IRErrorCode;
using clp_s::ffi::ir_stream::SerializationBuffer;
using clp_s::ffi::ir_stream::serialize_end_of_stream;
using clp_s::ffi::ir_stream::serialize_key_value_pair_record;
using clp_s::ffi::ir_stream::Value;
using clp_s::ffi::SchemaTree;
using clp_s::ffi::SchemaTreeNode;

namespace {
class Timer {
public:
    using TimePoint = decltype(std::chrono::high_resolution_clock::now());

    Timer() : m_start{std::chrono::high_resolution_clock::now()} {}

    [[nodiscard]] auto get_time_used_in_microsecond() const -> long long {
        auto const end{std::chrono::high_resolution_clock::now()};
        auto const elapsed{std::chrono::duration_cast<std::chrono::microseconds>(end - m_start)};
        return elapsed.count();
    }

private:
    TimePoint m_start;
};

size_t level_map[]{
        1 * 1024 * 1024,
        10 * 1024 * 1024,
        100 * 1024 * 1024,
        1024 * 1024 * 1024,
        10L * 1024 * 1024 * 1024
};
}  // namespace

auto benchmark(std::string_view input_path) -> int {
    size_t level{0};
    size_t raw_json_bytes{0};
    size_t msgpack_bytes{0};
    size_t ir_bytes{0};
    long long json_to_msgpack_time{0};
    long long msgpack_to_map_time{0};
    long long map_to_ir_time{0};
    long long ir_deserialize_time{0};
    long long map_to_msgpack_time{0};
    long long clp_ir_time{0};

    std::ifstream fin;
    fin.open(std::string(input_path));
    SerializationBuffer buffer;
    std::string line;
    std::vector<SchemaTreeNode::id_t> schema;
    std::vector<std::optional<clp_s::ffi::ir_stream::Value>> values;
    SchemaTree deserialized_schema_tree;
    size_t idx{0};
    std::optional<size_t> last_reported_level{std::nullopt};
    std::vector<int8_t> clp_ir_buf;
    std::string logtype;

    auto result_printer = [&]() -> void {
        nlohmann::json result;
        result.emplace("path", input_path);
        result.emplace("level", level + 1);
        result.emplace("num_lines", idx);
        result.emplace("size_json", raw_json_bytes);
        // result.emplace("size_msgpack", msgpack_bytes);
        // result.emplace("size_ir", ir_bytes);
        // result.emplace(
        //         "time_json_to_msgpack",
        //         static_cast<double>(json_to_msgpack_time) / 1000000.0
        // );
        // result.emplace("time_msgpack_to_map", static_cast<double>(msgpack_to_map_time) /
        // 1000000.0);
        result.emplace("time_map_to_ir", static_cast<double>(map_to_ir_time) / 1000000.0);
        // result.emplace(
        //         "time_ir_deserialization",
        //         static_cast<double>(ir_deserialize_time) / 1000000.0
        // );
        // result.emplace("time_map_to_msgpack", static_cast<double>(map_to_msgpack_time) /
        // 1000000.0);
        result.emplace("time_clp_ir", static_cast<double>(clp_ir_time) / 1000000.0);
        result.emplace("schema_tree_size", buffer.get_schema_tree().get_size());
        size_t max_depth{};
        size_t max_width{};
        // buffer.get_schema_tree().get_max_depth_and_width(max_depth, max_width);
        // result.emplace("schema_tree_max_depth", max_depth);
        // result.emplace("schema_tree_max_width", max_width);
        std::cerr << result.dump() << "\n";
        last_reported_level = level;
    };

    while (getline(fin, line)) {
        ++idx;
        raw_json_bytes += (line.size() + 1);  // New line character
        Timer json_to_msgpack_timer;
        nlohmann::json item = nlohmann::json::parse(line);
        auto const msgpack_data{nlohmann::json::to_msgpack(item)};
        json_to_msgpack_time += json_to_msgpack_timer.get_time_used_in_microsecond();

        Timer clp_ir_timer;
        if (false
            == clp::ffi::ir_stream::four_byte_encoding::serialize_message(
                    line,
                    logtype,
                    clp_ir_buf
            ))
        {
            std::cerr << "Failed to encode CLP message with idx " << idx << "\n";
            break;
        }
        clp_ir_time += clp_ir_timer.get_time_used_in_microsecond();
        clp_ir_buf.clear();

        msgpack_bytes += msgpack_data.size();
        Timer msgpack_to_map_timer;
        msgpack::object_handle oh;
        msgpack::unpack(
                oh,
                reinterpret_cast<char const*>(msgpack_data.data()),
                msgpack_data.size()
        );
        msgpack_to_map_time += msgpack_to_map_timer.get_time_used_in_microsecond();

        // Timer map_to_msgpack_timer;
        // msgpack::sbuffer sbuf;
        // msgpack::pack(sbuf, oh.get());
        // map_to_msgpack_time += map_to_msgpack_timer.get_time_used_in_microsecond();

        Timer map_to_ir_timer;
        if (false == serialize_key_value_pair_record(oh.get(), buffer)) {
            std::cerr << "Failed to serialize: (#" << idx << ")" << line << "\n";
            return -1;
        }
        map_to_ir_time += map_to_ir_timer.get_time_used_in_microsecond();
        auto const ir_buf{buffer.get_ir_buf()};
        ir_bytes += ir_buf.size();

        // clp::BufferReader buffer_reader(ir_buf.data(), ir_buf.size());
        // Timer ir_deserialize_timer;
        // if (IRErrorCode::Success
        //     != deserialize_next_key_value_pair_record(
        //             buffer_reader,
        //             deserialized_schema_tree,
        //             schema,
        //             values
        //     ))
        // {
        //     std::cerr << "Failed to deserialize (#" << idx << "): " << line << "\n";
        //     return -1;
        // }
        // ir_deserialize_time += ir_deserialize_timer.get_time_used_in_microsecond();
        buffer.flush_ir_buf();

        if (raw_json_bytes > level_map[level]) {
            result_printer();
            if (level == 4) {
                break;
            }
            ++level;
        }
    }
    if (false == last_reported_level.has_value() || last_reported_level.value() != level) {
        result_printer();
    }
    // if (buffer.get_schema_tree().get_size() != deserialized_schema_tree.get_size()) {
    //     std::cerr << "The deserialized tree's size doesn't match.\n";
    // }
    return 0;
}

auto compress(std::string_view input_path_view, size_t level) -> int {
    std::ifstream fin;
    std::string input_path{input_path_view};
    fin.open(std::string(input_path));
    SerializationBuffer buffer;
    std::string line;
    std::vector<SchemaTreeNode::id_t> schema;
    std::vector<std::optional<clp_s::ffi::ir_stream::Value>> values;
    SchemaTree deserialized_schema_tree;
    size_t idx{0};
    size_t raw_json_bytes{0};

    clp::FileWriter writer;
    writer.open(
            input_path + std::to_string(level) + ".clp.zst",
            clp::FileWriter::OpenMode::CREATE_FOR_WRITING
    );
    clp::streaming_compression::zstd::Compressor zstd_compressor;
    zstd_compressor.open(writer);

    while (getline(fin, line)) {
        ++idx;
        raw_json_bytes += (line.size() + 1);  // New line character
        nlohmann::json item = nlohmann::json::parse(line);
        auto const msgpack_data{nlohmann::json::to_msgpack(item)};
        msgpack::object_handle oh;
        msgpack::unpack(
                oh,
                reinterpret_cast<char const*>(msgpack_data.data()),
                msgpack_data.size()
        );

        if (false == serialize_key_value_pair_record(oh.get(), buffer)) {
            std::cerr << "Failed to serialize: (#" << idx << ")" << line << "\n";
            return -1;
        }
        auto const ir_buf{buffer.get_ir_buf()};
        zstd_compressor.write(ir_buf.data(), ir_buf.size());
        buffer.flush_ir_buf();

        if (raw_json_bytes > level_map[level]) {
            break;
        }
    }

    zstd_compressor.close();
    writer.close();
    return 0;
}

auto compress_raw(std::string_view input_path_view, size_t level) -> int {
    std::ifstream fin;
    std::string input_path{input_path_view};
    fin.open(std::string(input_path));
    SerializationBuffer buffer;
    std::string line;
    std::vector<SchemaTreeNode::id_t> schema;
    std::vector<std::optional<clp_s::ffi::ir_stream::Value>> values;
    SchemaTree deserialized_schema_tree;
    size_t idx{0};
    size_t raw_json_bytes{0};

    clp::FileWriter writer_json;
    writer_json.open(
            input_path + std::to_string(level) + ".zst",
            clp::FileWriter::OpenMode::CREATE_FOR_WRITING
    );
    clp::streaming_compression::zstd::Compressor zstd_compressor_json;
    zstd_compressor_json.open(writer_json);

    clp::FileWriter writer_msgpack;
    writer_msgpack.open(
            input_path + std::to_string(level) + ".msgpack.zst",
            clp::FileWriter::OpenMode::CREATE_FOR_WRITING
    );
    clp::streaming_compression::zstd::Compressor zstd_compressor_msgpack;
    zstd_compressor_msgpack.open(writer_msgpack);

    while (getline(fin, line)) {
        ++idx;
        raw_json_bytes += (line.size() + 1);  // New line character
        nlohmann::json item = nlohmann::json::parse(line);
        auto const msgpack_data{nlohmann::json::to_msgpack(item)};
        line += "\n";
        zstd_compressor_json.write(line.data(), line.size());
        zstd_compressor_msgpack.write(
                reinterpret_cast<char const*>(msgpack_data.data()),
                msgpack_data.size()
        );

        if (raw_json_bytes > level_map[level]) {
            break;
        }
    }

    zstd_compressor_json.close();
    zstd_compressor_msgpack.close();
    writer_json.close();
    writer_msgpack.close();

    return 0;
}

auto deserialize(std::string input_path) -> int {
    clp::FileWriter writer;
    writer.open(input_path + ".json", clp::FileWriter::OpenMode::CREATE_FOR_WRITING);

    clp::streaming_compression::zstd::Decompressor zstd_reader;
    zstd_reader.open(input_path);

    SchemaTree schema_tree;
    std::vector<SchemaTreeNode::id_t> schema;
    std::vector<std::optional<clp_s::ffi::ir_stream::Value>> values;
    std::string json_str;

    std::string line;
    size_t idx{0};
    bool failed{false};
    while (true) {
        auto const err{
                deserialize_next_key_value_pair_record(zstd_reader, schema_tree, schema, values)
        };
        if (IRErrorCode::EndOfStream == err) {
            break;
        }
        if (IRErrorCode::Success != err) {
            std::cerr << idx << " Failed to deserialize IR to kv pairs.\n";
            return -1;
        }
        if (false == deserialize_record_as_json_str(schema_tree, schema, values, json_str)) {
            std::cerr << idx << " Failed to serialize JSON string from kv pairs.\n";
            return -1;
        }
        json_str.push_back('\n');
        writer.write_string(json_str);
        ++idx;
    }
    writer.close();
    return 0;
}

auto main(int argc, char const* argv[]) -> int {
    if (1 >= argc) {
        std::cerr << "Error: Incorrect Args.\n";
    }
    std::string_view input_path{argv[1]};
    // return benchmark(input_path);
    // int ret{};
    // ret = compress_raw(input_path, 3);
    // if (0 != ret) {
    //     return ret;
    // }
    // ret = compress_raw(input_path, 4);
    // if (0 != ret) {
    //     return ret;
    // }
    deserialize(std::string{input_path.begin(), input_path.end()});
    return 0;
}
