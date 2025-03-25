from typing import List, Set

from stock_market_research_kit.session import SessionName, Session, SessionType
from stock_market_research_kit.sessions_sequence import SessionsSequence


class TreeNode:
    def __init__(self, key, parent, count):
        self.key = key
        self.count = count
        self.parent = parent
        self.children = []

    def traverse_children(self):
        yield self
        for child in self.children:
            yield from child.traverse_children()

    def traverse_parents(self):
        yield self
        if self.parent:
            yield from self.parent.traverse_parents()

    # def get_paths(self):
    #     def dfs(node, path, res):
    #         path.append((node.key, node.count))
    #         if not node.children:
    #             res.append(path[:])
    #         else:
    #             for child in node.children:
    #                 dfs(child, path, res)
    #         path.pop()
    #
    #     result = []
    #     dfs(self, [], result)
    #     return result

    def get_paths(self):
        def dfs(node, path, res, seen):
            path.append((node.key, node.count))
            path_tuple = tuple(path)
            if path_tuple not in seen:
                res.append(path[:])
                seen.add(path_tuple)
            for child in node.children:
                dfs(child, path, res, seen)
            path.pop()

        result = []
        dfs(self, [], result, set())
        return result


class Tree:
    def __init__(self, name, key, count):
        self.name = name
        self.root = TreeNode(key, None, count)

    def insert(self, parent_node_key, key):
        for parent_node in self.root.traverse_children():
            if parent_node.key != parent_node_key:
                continue

            same_key_node = next((ch for ch in parent_node.children if ch.key == key), None)
            if same_key_node:
                same_key_node.count += 1
            else:
                parent_node.children.append(TreeNode(key, parent_node, 1))

            # for node in parent_node.traverse_parents():
            #     node.count += 1
            self.root.count += 1

            return True
        return False

    def insert_subtree(self, parent_node_key, subtree):
        parent_node = self.find_by_key(parent_node_key)
        if not parent_node:
            return False

        current_parent = parent_node

        for subtree_node in subtree.traverse_children():
            subtree_node.parent = current_parent

            same_key_node = next((ch for ch in current_parent.children if ch.key == subtree_node.key), None)
            if same_key_node:
                same_key_node.count += 1
                current_parent = same_key_node
            else:
                new_node = TreeNode(
                    subtree_node.key,
                    current_parent,
                    1
                )
                current_parent.children.append(new_node)
                current_parent = new_node

            self.root.count += 1
        return True

    # WARN! keys by design could be not unique in tree
    def find_by_key(self, key):
        for node in self.root.traverse_children():
            if node.key == key:
                return node
        return None

    def find_by_path(self, path):
        if len(path) == 0:
            return None
        if path[0] != self.root.key:
            return None
        node = self.root
        for key in path[1:]:
            node = next((child for child in node.children if child.key == key), None)
            if node is None:
                return None
        return node


def flatten_candle_tree(tree: Tree):
    if tree.root.count == 0:
        return []

    result = []

    for node in tree.root.traverse_children():
        if node.key == tree.root.key:
            continue

        parent_session = None if node.parent.key == tree.root.key else node.parent.key.split('__')[0]
        parent_candle_type = None if node.parent.key == tree.root.key else node.parent.key.split('__')[1]
        [session, candle_type] = node.key.split('__')

        result.append(SessionsSequence(
            session=session,
            candle_type=candle_type,
            parent_session=parent_session,
            parent_candle_type=parent_candle_type,
            count=node.count
        ))

    return result


def tree_from_sessions(name: str, all_sessions: List[Session], sessions_in_order: List[SessionName],
                       first_session_candle_types: Set[SessionType]):
    tree = Tree(name, 'total', 0)

    day_groups = []
    for session in all_sessions:
        if len(day_groups) == 0 or day_groups[-1]['day_date'] != session.day_date:
            day_groups.append({'day_date': session.day_date})
            day_groups[-1][session.name.value] = session
            continue
        day_groups[-1][session.name.value] = session

    for day_group in day_groups:
        ordered = []
        for session_name in sessions_in_order:
            if session_name.value not in day_group:
                break
            ordered.append(day_group[session_name.value])

        if len(ordered) < len(sessions_in_order):
            continue

        if ordered[0].type not in first_session_candle_types:
            continue

        subtree = None
        for i in range(len(ordered) - 1, -1, -1):
            s = ordered[i]
            new_subtree = TreeNode(f"{s.name.value}__{s.type.value}", None, 1)
            if subtree:
                subtree.parent = new_subtree
                new_subtree.children = [subtree]
            subtree = new_subtree
        tree.insert_subtree('total', subtree)

    return tree


def tree_from_sequences(name: str, sequences: List[SessionsSequence]):
    pass


if __name__ == "__main__":
    try:
        test_tree = Tree('test_tree', 'total', 0)
        test_tree.insert('total', 'CME__BULL')
        test_tree.insert('total', 'CME__BEAR')
        test_tree.insert('CME__BEAR', 'ASIA__BULL')
        test_tree.insert('CME__BEAR', 'ASIA__BULL')

        test_subtree = TreeNode('CME__BEAR', None, 1)
        test_subtree.children = [TreeNode('ASIA__BULL', test_subtree, 1)]

        test_tree.insert_subtree('total', test_subtree)

        print([(x.key, x.count) for x in test_tree.find_by_key('total').traverse_children()])
        print([(x.key, x.count) for x in test_tree.find_by_key('ASIA__BULL').traverse_parents()])
        print(flatten_candle_tree(test_tree))
        test_tree.root.get_paths()
        print(flatten_candle_tree(test_tree))
        print('find_by_path', test_tree.find_by_path(['total', 'CME__BEAR', 'ASIA__BULL']))
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
