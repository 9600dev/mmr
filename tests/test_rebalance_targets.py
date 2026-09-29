"""D08: the targets loader accepts exactly mmr.targets.v1 and refuses everything else, naming the field."""
from __future__ import annotations

import datetime as dt
import json

import pytest

from rebalance_fakes import SHA, make_doc, write_targets
from trader.rebalance.targets import (
    SCHEMA, TargetsError, latest_targets_path, load_targets, validate_targets,
)


def test_valid_file_round_trips(tmp_path):
    path = write_targets(tmp_path)
    t = load_targets(path)
    assert t.sleeve == 's2p' and t.group == 'sleeve_s2p'
    assert t.as_of == dt.date(2026, 9, 28) and t.source == 'rebalance:s2p:2026-09-28'
    assert t.generated_at.tzinfo is not None and t.generated_at.utcoffset() == dt.timedelta(0)
    assert t.strategy.class_name == 'S2PTopQuintile' and t.strategy.source_sha256 == SHA
    assert t.capital_usd == 10_000.0 and t.weights == {'AAPL': 0.5, 'MSFT': 0.3}
    assert abs(t.weight_sum - 0.8) < 1e-12 and t.path == str(path)


def test_schema_constant_matches_d08():
    assert SCHEMA == 'mmr.targets.v1'


@pytest.mark.parametrize('mutation, needle', [
    ({'schema': 'mmr.targets.v2'}, 'schema'),
    ({'sleeve': 'S2P'}, 'sleeve'),
    ({'sleeve': 's2p-live'}, 'sleeve'),
    ({'sleeve': '2p'}, 'sleeve'),
    ({'sleeve': ''}, 'sleeve'),
    ({'as_of': '28/09/2026'}, 'as_of'),
    ({'as_of': 20260928}, 'as_of'),
    ({'generated_at': 'yesterday'}, 'generated_at'),
    ({'generated_at': '2026-09-28T21:05:00'}, 'generated_at'),      # no UTC offset
    ({'prices_as_of': '2026-09-29'}, 'prices_as_of'),                # after as_of
    ({'capital_usd': 0}, 'capital_usd'),
    ({'capital_usd': -5.0}, 'capital_usd'),
    ({'capital_usd': 'ten grand'}, 'capital_usd'),
    ({'capital_usd': float('inf')}, 'capital_usd'),
    ({'weights': {}}, 'weights'),
    ({'weights': {'AAPL': 0.5, 'MSFT': -0.1}}, 'long-only'),
    ({'weights': {'AAPL': 0.0}}, 'long-only'),
    ({'weights': {'AAPL': 1.2}}, '> 1.0'),
    ({'weights': {'AAPL': 0.6, 'MSFT': 0.5}}, 'sum'),
    ({'weights': {'aapl': 0.5}}, 'symbol'),
    ({'weights': {'AAPL': '0.5'}}, 'number'),
    ({'weights': {'AAPL': float('nan')}}, 'finite'),
    ({'weights': ['AAPL']}, 'weights'),
    ({'notes': 7}, 'notes'),
])
def test_malformed_documents_are_refused_with_the_field_named(mutation, needle):
    with pytest.raises(TargetsError) as exc:
        validate_targets(make_doc(**mutation), path='t.json')
    assert needle in str(exc.value)
    assert 't.json' in str(exc.value)


@pytest.mark.parametrize('missing', ['schema', 'sleeve', 'as_of', 'generated_at', 'strategy',
                                     'capital_usd', 'prices_as_of', 'weights'])
def test_missing_required_fields_are_refused(missing):
    doc = make_doc()
    del doc[missing]
    with pytest.raises(TargetsError, match=missing):
        validate_targets(doc)


@pytest.mark.parametrize('field', ['family', 'id', 'module', 'class', 'params', 'source_sha256'])
def test_strategy_provenance_is_required(field):
    doc = make_doc()
    del doc['strategy'][field]
    with pytest.raises(TargetsError, match=field):
        validate_targets(doc)


def test_strategy_sha_must_be_sha256_hex():
    with pytest.raises(TargetsError, match='source_sha256'):
        validate_targets(make_doc(strategy={**make_doc()['strategy'], 'source_sha256': 'abc'}))


def test_weight_sum_exactly_one_is_accepted():
    t = validate_targets(make_doc(weights={'AAPL': 0.1, 'MSFT': 0.2, 'GOOGL': 0.7}))
    assert t.weight_sum == 1.0
    validate_targets(make_doc(weights={'AAPL': 0.5, 'MSFT': 0.5}))
    validate_targets(make_doc(weights={'AAPL': 1.0}))


@pytest.mark.parametrize('weights', [
    {'AAPL': 0.5, 'MSFT': 0.5001},
    {'AAPL': 1.0000000001},                      # the round-1 critic's reproduction: no tolerance (D08)
    {'AAPL': 0.5, 'MSFT': 0.5000000001},
    {'AAPL': 0.5, 'MSFT': 0.5 + 2 ** -52},        # one ulp above 1.0 in float
])
def test_any_weight_sum_above_one_is_refused_strictly(weights):
    with pytest.raises(TargetsError, match='sum|> 1.0'):
        validate_targets(make_doc(weights=weights))


def test_file_path_judges_the_sum_on_the_decimals_as_written(tmp_path):
    # 0.1 + 0.2 + 0.7 is exactly 1.0 in decimal and must load; 1.0000000001 must not.
    ok = tmp_path / 'ok.json'
    ok.write_text(json.dumps(make_doc()).replace('{"AAPL": 0.5, "MSFT": 0.3}',
                                                 '{"AAPL": 0.1, "MSFT": 0.2, "GOOGL": 0.7}'))
    assert load_targets(ok).weight_sum == 1.0
    bad = tmp_path / 'bad.json'
    bad.write_text(json.dumps(make_doc()).replace('{"AAPL": 0.5, "MSFT": 0.3}', '{"AAPL": 1.0000000001}'))
    with pytest.raises(TargetsError, match='1.0000000001 > 1.0'):
        load_targets(bad)
    two = tmp_path / 'two.json'
    two.write_text(json.dumps(make_doc()).replace('{"AAPL": 0.5, "MSFT": 0.3}', '{"AAPL": 0.5, "MSFT": 0.5000000001}'))
    with pytest.raises(TargetsError, match='sum to 1.0000000001 > 1.0'):
        load_targets(two)
    tiny = tmp_path / 'tiny.json'
    tiny.write_text(json.dumps(make_doc()).replace('{"AAPL": 0.5, "MSFT": 0.3}',
                                                   '{"AAPL": 0.5, "MSFT": 0.5000000000000000000001}'))
    with pytest.raises(TargetsError, match='sum'):
        load_targets(tiny)


def test_file_path_numbers_come_back_json_native(tmp_path):
    # Regression: parse_float=Decimal leaked Decimals into strategy.params, and the proposal store's
    # json.dumps of the metadata then failed every propose ("Object of type Decimal is not JSON serializable").
    doc = make_doc()
    doc['strategy']['params'] = {'quantile': 0.2, 'lookback': 12, 'nested': {'x': [1.5, 2]}}
    t = load_targets(write_targets(tmp_path, doc))
    assert type(t.capital_usd) is float and all(type(w) is float for w in t.weights.values())
    assert t.strategy.params == {'quantile': 0.2, 'lookback': 12, 'nested': {'x': [1.5, 2]}}
    assert type(t.strategy.params['quantile']) is float and type(t.strategy.params['lookback']) is int
    json.dumps(t.strategy.params); json.dumps(t.raw)


def test_dotted_and_hyphenated_us_symbols_are_accepted():
    t = validate_targets(make_doc(weights={'BRK.B': 0.4, 'BF-B': 0.3}))
    assert set(t.weights) == {'BRK.B', 'BF-B'}


def test_top_level_must_be_an_object():
    with pytest.raises(TargetsError, match='object'):
        validate_targets(['not', 'an', 'object'])


def test_missing_file_and_invalid_json_are_refused(tmp_path):
    with pytest.raises(TargetsError, match='not found'):
        load_targets(tmp_path / 'nope.json')
    bad = tmp_path / 'bad.json'
    bad.write_text('{"schema": ', encoding='utf-8')
    with pytest.raises(TargetsError, match='JSON'):
        load_targets(bad)


def test_generated_at_accepts_explicit_offsets_and_normalises_to_utc():
    t = validate_targets(make_doc(generated_at='2026-09-28T17:05:00-04:00'))
    assert t.generated_at == dt.datetime(2026, 9, 28, 21, 5, tzinfo=dt.timezone.utc)


def test_latest_targets_path_picks_newest_as_of_and_ignores_other_files(tmp_path):
    assert latest_targets_path('s2p', tmp_path) is None
    write_targets(tmp_path, make_doc(as_of='2026-08-31', prices_as_of='2026-08-31'), '2026-08-31.json', sleeve_dir=True)
    newest = write_targets(tmp_path, make_doc(), '2026-09-28.json', sleeve_dir=True)
    (tmp_path / 's2p' / 'notes.json').write_text('{}')
    (tmp_path / 's2p' / 'README.md').write_text('x')
    assert latest_targets_path('s2p', tmp_path) == newest
    with pytest.raises(TargetsError, match='sleeve'):
        latest_targets_path('../etc', tmp_path)


def test_default_targets_dir_honours_env(monkeypatch, tmp_path):
    from trader.rebalance.targets import default_targets_dir
    monkeypatch.setenv('MMR_TARGETS_DIR', str(tmp_path))
    assert default_targets_dir() == tmp_path
    monkeypatch.delenv('MMR_TARGETS_DIR')
    assert default_targets_dir().as_posix().endswith('.local/share/mmr/data/targets')


def test_raw_document_is_preserved_for_audit():
    doc = make_doc()
    t = validate_targets(doc)
    assert json.dumps(t.raw, sort_keys=True) == json.dumps(doc, sort_keys=True)
