from typing import List, Set

from stock_market_research_kit.session import SessionName, Session, SessionType
from stock_market_research_kit.sessions_sequence import SessionsSequence


class TreeNode:
    def __init__(self, key, parent, count):
        self.key = key
        self.count = count
        self.parent = parent
        self.children = []


class Tree:
    def __init__(self, name, key, count):
        self.name = name
        self.root = TreeNode(key, None, count)

    def traverse_children(self, node):
        yield node
        for child in node.children:
            yield from self.traverse_children(child)

    def traverse_parents(self, node):
        yield node
        if node.parent:
            yield from self.traverse_parents(node.parent)

    def insert(self, parent_node_key, key):
        for parent_node in self.traverse_children(self.root):
            if parent_node.key != parent_node_key:
                continue

            same_key_node = next((ch for ch in parent_node.children if ch.key == key), None)
            if same_key_node:
                same_key_node.count += 1
            else:
                parent_node.children.append(TreeNode(key, parent_node, 1))

            for node in self.traverse_parents(parent_node):
                node.count += 1

            return True
        return False

    def find(self, key):
        for node in self.traverse_children(self.root):
            if node.key == key:
                return node
        return None


def flatten_candle_tree(tree: Tree):
    if tree.root.count == 0:
        return []

    result = []

    for node in tree.traverse_children(tree.root):
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

        for i in range(len(ordered)):
            s = ordered[i]
            if i == 0:
                if s.type not in first_session_candle_types:
                    break
                tree.insert('total', f"{s.name}__{s.type}")
            else:
                tree.insert(f"{ordered[i - 1].name}__{ordered[i - 1].type}", f"{s.name}__{s.type}")

    return tree


def tree_from_sequences(name: str, sequences: List[SessionsSequence]):
    pass


if __name__ == "__main__":
    try:
        test_tree = Tree('total', 0)
        test_tree.insert('total', 'CME__BULL')
        test_tree.insert('total', 'CME__BEAR')
        test_tree.insert('CME__BEAR', 'ASIA__BULL')
        test_tree.insert('CME__BEAR', 'ASIA__BULL')

        print([(x.key, x.count) for x in test_tree.traverse_children(test_tree.find('total'))])
        print([(x.key, x.count) for x in test_tree.traverse_parents(test_tree.find('ASIA__BULL'))])
        print(flatten_candle_tree(test_tree))
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
