#include <Catch2/single_include/catch2/catch.hpp>

#include "../src/clp_s/ffi/SchemaTree.hpp"
#include "../src/clp_s/ffi/SchemaTreeNode.hpp"

using clp_s::ffi::SchemaTree;
using clp_s::ffi::SchemaTreeNode;

TEST_CASE("schema_tree", "[ffi]") {
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
