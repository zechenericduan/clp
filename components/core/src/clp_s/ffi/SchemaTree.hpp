#ifndef CLP_S_FFI_SCHEMA_TREE_HPP
#define CLP_S_FFI_SCHEMA_TREE_HPP

#include <string>
#include <string_view>
#include <vector>

#include <absl/container/flat_hash_map.h>

#include "../TraceableException.hpp"
#include "SchemaTreeNode.hpp"

namespace clp_s::ffi {
/**
 * This class implements a schema tree used in the CLP IR stream.
 */
class SchemaTree {
public:
    class Exception : public TraceableException {
    public:
        Exception(
                ErrorCode error_code,
                char const* const filename,
                int line_number,
                std::string message
        )
                : TraceableException(error_code, filename, line_number),
                  m_message(std::move(message)) {}

        [[nodiscard]] auto what() const noexcept -> char const* override {
            return m_message.c_str();
        }

    private:
        std::string m_message;
    };

    /**
     * When locating a tree node from a tree, we might not always have the tree node id as the
     * index. Instead, we usually use the parent id, the key name, and the node type to locate a
     * unique tree node in the tree. This class wraps the location information as a none-integer
     * unique identifier of a tree node.
     */
    class TreeNodeLocator {
    public:
        TreeNodeLocator(
                SchemaTreeNode::id_t parent_id,
                std::string_view key_name,
                SchemaTreeNode::Type type
        )
                : m_tuple{parent_id, {key_name.begin(), key_name.end()}, type} {};
        TreeNodeLocator(TreeNodeLocator const&) = default;

        [[nodiscard]] auto get_parent_id() const -> SchemaTreeNode::id_t {
            return std::get<0>(m_tuple);
        }

        [[nodiscard]] auto get_key_name() const -> std::string_view { return std::get<1>(m_tuple); }

        [[nodiscard]] auto get_type() const -> SchemaTreeNode::Type { return std::get<2>(m_tuple); }

        [[nodiscard]] auto get_tuple() const
                -> std::tuple<SchemaTreeNode::id_t, std::string, SchemaTreeNode::Type> const& {
            return m_tuple;
        }

    private:
        std::tuple<SchemaTreeNode::id_t, std::string, SchemaTreeNode::Type> m_tuple;
    };

    static constexpr SchemaTreeNode::id_t cRootId{0};

    SchemaTree() { m_tree_nodes.emplace_back(cRootId, cRootId, "", SchemaTreeNode::Type::Obj); }

    ~SchemaTree() = default;
    SchemaTree(SchemaTree const&) = delete;
    auto operator=(SchemaTree const&) -> SchemaTree& = delete;
    SchemaTree(SchemaTree&&) = default;
    auto operator=(SchemaTree&&) -> SchemaTree& = default;

    [[nodiscard]] auto get_size() const -> size_t { return m_tree_nodes.size(); }

    /**
     * @return The tree node with the given id.
     * @throw Exception if the given id is not valid (i.e., out of bound).
     */
    [[nodiscard]] auto get_node_with_id(SchemaTreeNode::id_t id) const -> SchemaTreeNode const& {
        if (m_tree_nodes.size() <= static_cast<size_t>(id)) {
            throw Exception(
                    ErrorCodeOutOfBounds,
                    __FILE__,
                    __LINE__,
                    "The given tree node id is invalid."
            );
        }
        return m_tree_nodes[id];
    }

    /**
     * Checks if a node exists with the given parent id, key name, and type.
     * @param location Locator to locate the unique tree node.
     * @param node_id Returns the node id if the node exists.
     * @return true if the node exists, false otherwise.
     */
    [[nodiscard]] auto
    has_node(TreeNodeLocator const& location, SchemaTreeNode::id_t& node_id) const -> bool {
        auto const it{m_node_map.find(location.get_tuple())};
        if (m_node_map.end() == it) {
            return false;
        }
        node_id = it->second;
        return true;
    }

    /**
     * Inserts a new node to the given location.
     * @param location
     * @return The node id of the inserted node.
     * @throw Exception if the node with given location already exists.
     */
    [[maybe_unused]] auto insert_node(TreeNodeLocator const& location) -> SchemaTreeNode::id_t {
        SchemaTreeNode::id_t node_id{};
        // if (has_node(location, node_id)) {
        //     throw Exception(ErrorCodeFailure, __FILE__, __LINE__, "Tree Node already exists.");
        // }
        node_id = m_tree_nodes.size();
        m_tree_nodes.emplace_back(
                node_id,
                location.get_parent_id(),
                location.get_key_name(),
                location.get_type()
        );
        m_tree_nodes[location.get_parent_id()].add_child(node_id);
        m_node_map.emplace(location.get_tuple(), node_id);
        return node_id;
    }

    /**
     * Takes a snapshot of the current schema tree for potential recovery on failure.
     */
    auto take_snapshot() -> void { m_snapshot_size = m_tree_nodes.size(); }

    /**
     * Reverts the tree to the time when the snapshot was taken.
     * @throw Exception if the snapshot was not taken.
     */
    auto revert() -> void {
        if (0 == m_snapshot_size) {
            throw Exception(
                    ErrorCodeFailure,
                    __FILE__,
                    __LINE__,
                    "Snapshot was not taken before calling revert."
            );
        }
        while (m_tree_nodes.size() != m_snapshot_size) {
            auto const& node{m_tree_nodes.back()};
            m_tree_nodes[node.get_parent_id()].remove_last_inserted_child();
            m_tree_nodes.pop_back();
        }
        m_snapshot_size = 0;
    }

    /**
     * Resets the schema tree by removing all the tree nodes except root.
     */
    auto reset() -> void {
        m_snapshot_size = 0;
        m_tree_nodes.clear();
        m_tree_nodes.emplace_back(cRootId, cRootId, "", SchemaTreeNode::Type::Obj);
    }

    auto dump() -> std::string {
        size_t idx{0};
        std::string result;
        for (auto const& node : m_tree_nodes) {
            result += std::to_string(idx);
            result += "|";
            result += std::to_string(node.get_parent_id());
            result += " ";
            result += node.get_key_name();
            result += "\n";
            ++idx;
        }
        return result;
    }

    auto get_max_depth_and_width(size_t& max_depth, size_t& max_width) const -> void {
        max_depth = 0;
        max_width = 0;
        std::vector<std::pair<size_t, size_t>> stack;
        stack.emplace_back(cRootId, 0);
        while (false == stack.empty()) {
            auto const [id, depth]{stack.back()};
            stack.pop_back();
            max_depth = max_depth < depth ? depth : max_depth;
            auto const& children{m_tree_nodes[id].get_children_ids()};
            max_width = max_width < children.size() ? children.size() : max_width;
            for (auto const i : children) {
                stack.emplace_back(i, depth + 1);
            }
        }
    }

private:
    size_t m_snapshot_size{0};
    std::vector<SchemaTreeNode> m_tree_nodes;
    absl::flat_hash_map<
            std::tuple<SchemaTreeNode::id_t, std::string, SchemaTreeNode::Type>,
            SchemaTreeNode::id_t>
            m_node_map;
};
}  // namespace clp_s::ffi
#endif
