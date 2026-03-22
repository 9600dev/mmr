"""Unit tests for trader.data.position_groups."""

import pytest

from trader.data.position_groups import PositionGroupStore


class TestPositionGroupStore:
    def test_create_group(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        group = store.create_group('mining', description='Mining stocks', max_allocation_pct=0.20)
        assert group.name == 'mining'
        assert group.description == 'Mining stocks'
        assert group.max_allocation_pct == 0.20
        assert group.created_at is not None

    def test_create_duplicate_raises(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        with pytest.raises(ValueError, match='already exists'):
            store.create_group('mining')

    def test_delete_group(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        assert store.delete_group('mining') is True
        assert store.get_group('mining') is None

    def test_delete_nonexistent(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        assert store.delete_group('nonexistent') is False

    def test_delete_group_removes_members(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        store.add_member('mining', 'BHP')
        store.add_member('mining', 'RIO')
        store.delete_group('mining')
        assert store.get_members('mining') == []

    def test_get_group(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('tech', max_allocation_pct=0.30)
        group = store.get_group('tech')
        assert group is not None
        assert group.name == 'tech'
        assert group.max_allocation_pct == 0.30

    def test_get_group_not_found(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        assert store.get_group('nonexistent') is None

    def test_list_groups(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        store.create_group('tech')
        store.create_group('defensive')
        groups = store.list_groups()
        names = [g.name for g in groups]
        assert 'defensive' in names
        assert 'mining' in names
        assert 'tech' in names
        assert len(groups) == 3

    def test_list_groups_empty(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        assert store.list_groups() == []

    def test_update_group(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining', max_allocation_pct=0.10)
        result = store.update_group('mining', max_allocation_pct=0.25, description='Updated')
        assert result is True
        group = store.get_group('mining')
        assert group.max_allocation_pct == 0.25
        assert group.description == 'Updated'

    def test_update_nonexistent(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        assert store.update_group('nonexistent', max_allocation_pct=0.5) is False


class TestGroupMembers:
    def test_add_member(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        assert store.add_member('mining', 'BHP') is True
        assert store.get_members('mining') == ['BHP']

    def test_add_member_to_nonexistent_group(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        assert store.add_member('nonexistent', 'BHP') is False

    def test_add_duplicate_member(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        store.add_member('mining', 'BHP')
        assert store.add_member('mining', 'BHP') is False

    def test_remove_member(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        store.add_member('mining', 'BHP')
        assert store.remove_member('mining', 'BHP') is True
        assert store.get_members('mining') == []

    def test_remove_nonexistent_member(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        assert store.remove_member('mining', 'BHP') is False

    def test_get_members_sorted(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        store.add_member('mining', 'RIO')
        store.add_member('mining', 'BHP')
        store.add_member('mining', 'FMG')
        assert store.get_members('mining') == ['BHP', 'FMG', 'RIO']

    def test_get_groups_for_symbol(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        store.create_group('asx')
        store.add_member('mining', 'BHP')
        store.add_member('asx', 'BHP')
        groups = store.get_groups_for_symbol('BHP')
        assert sorted(groups) == ['asx', 'mining']

    def test_get_groups_for_symbol_not_found(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        assert store.get_groups_for_symbol('AAPL') == []

    def test_get_all_memberships(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        store.create_group('tech')
        store.add_member('mining', 'BHP')
        store.add_member('mining', 'RIO')
        store.add_member('tech', 'AAPL')
        memberships = store.get_all_memberships()
        assert memberships == {'mining': ['BHP', 'RIO'], 'tech': ['AAPL']}

    def test_group_includes_members(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        store.add_member('mining', 'BHP')
        store.add_member('mining', 'RIO')
        group = store.get_group('mining')
        assert sorted(group.members) == ['BHP', 'RIO']

    def test_list_groups_includes_members(self, tmp_duckdb_path):
        store = PositionGroupStore(tmp_duckdb_path)
        store.create_group('mining')
        store.add_member('mining', 'BHP')
        groups = store.list_groups()
        assert groups[0].members == ['BHP']
