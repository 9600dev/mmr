"""SPEC: a backtest may only see what had been filed.

Fundamental data carries two timestamps and using the wrong one manufactures
an enormous fake edge. A quarter ENDS on one date and is FILED 18-60 days
later (median 34, measured over 499 records). Ranking companies by an earnings
figure on its period-end date means trading on results that were not public
for another month, and the resulting equity curve looks wonderful.

This is the same failure as a misaligned forward return, one layer up, and
just as invisible: the output is a well-formed table of real numbers about
real companies. Only the dates are wrong.

So the properties here are about time, not about finance:

  * a filing is visible strictly AFTER its acceptance, never at or before it;
  * a filing that cannot be placed in time is visible at NO time;
  * later ACCEPTANCE wins, which is what makes restatements behave without
    special-casing - an amendment is just a filing accepted later.
"""

from __future__ import annotations

import datetime as dt

import pytest

from hypothesis import given, settings, strategies as st

from trader.data.fundamentals import (
    Filing, extract_concepts, is_knowable_at, latest_known, reporting_lag_days,
    safe_ratio)


UTC = dt.timezone.utc


def _f(conid=1, accepted='2026-05-01T06:01:00+00:00', period='2026-03-28',
       amend=False, acc='X'):
    return Filing(conid=conid, cik='320193', ticker='AAPL',
                  form_type='10-Q/A' if amend else '10-Q', accession_no=acc,
                  accepted_at=dt.datetime.fromisoformat(accepted),
                  period_end=dt.date.fromisoformat(period) if period else None,
                  is_amendment=amend)


class TestVisibilityIsStrictlyAfterAcceptance:

    def test_a_filing_is_invisible_before_it_is_accepted(self):
        f = _f()
        assert not is_knowable_at(f.accepted_at,
                                  dt.datetime(2026, 4, 30, tzinfo=UTC))

    def test_a_filing_is_visible_after_it_is_accepted(self):
        f = _f()
        assert is_knowable_at(f.accepted_at,
                              dt.datetime(2026, 5, 2, tzinfo=UTC))

    def test_the_exact_instant_of_acceptance_is_NOT_visible(self):
        """The boundary is where an off-by-one hides, and it fires on exactly
        the days that matter - when news lands. A filing accepted at the
        instant you are asking about was not available to act on then."""
        f = _f()
        assert not is_knowable_at(f.accepted_at, f.accepted_at)

    def test_a_filing_with_no_acceptance_time_is_visible_at_no_time(self):
        """It cannot be placed in time. Admitting it 'because it is probably
        old enough' lets unplaceable records into precisely the periods where
        they do the most damage."""
        for when in (dt.datetime(1990, 1, 1, tzinfo=UTC),
                     dt.datetime(2099, 1, 1, tzinfo=UTC)):
            assert not is_knowable_at(None, when)

    def test_a_naive_timestamp_is_refused_rather_than_assumed(self):
        """Comparing naive to aware raises; guessing a timezone would be wrong
        by up to a day around the close, which is where the value is."""
        naive = dt.datetime(2026, 5, 1, 6, 1)
        assert not is_knowable_at(naive, dt.datetime(2026, 6, 1, tzinfo=UTC))

    @settings(max_examples=300, deadline=None)
    @given(offset=st.integers(min_value=-500, max_value=500))
    def test_visibility_is_monotone_in_time(self, offset):
        """Once visible, always visible. A filing cannot un-file itself."""
        f = _f()
        t = f.accepted_at + dt.timedelta(days=offset)
        if is_knowable_at(f.accepted_at, t):
            assert is_knowable_at(f.accepted_at, t + dt.timedelta(days=1))


class TestRestatementsNeedNoSpecialCase:

    def test_an_amendment_is_invisible_until_it_is_filed(self):
        """The scenario that silently rewrites history: a Q2 restated in
        November. A database that overwrites the original makes a July
        backtest trade on numbers that did not exist until November."""
        original = _f(acc='orig', accepted='2026-08-01T10:00:00+00:00')
        amended = _f(acc='amend', accepted='2026-11-15T10:00:00+00:00',
                     amend=True)
        in_september = latest_known([original, amended],
                                    dt.datetime(2026, 9, 1, tzinfo=UTC))
        assert in_september[1].accession_no == 'orig'

    def test_after_filing_the_amendment_wins(self):
        original = _f(acc='orig', accepted='2026-08-01T10:00:00+00:00')
        amended = _f(acc='amend', accepted='2026-11-15T10:00:00+00:00',
                     amend=True)
        in_december = latest_known([original, amended],
                                   dt.datetime(2026, 12, 1, tzinfo=UTC))
        assert in_december[1].accession_no == 'amend'

    def test_later_acceptance_wins_not_later_period(self):
        """A filing covering an OLDER period but accepted LATER is the more
        current knowledge. Sorting by period would prefer the stale one."""
        newer_period = _f(acc='p2', period='2026-06-30',
                          accepted='2026-08-01T10:00:00+00:00')
        older_period_later_filed = _f(acc='p1', period='2026-03-28',
                                      accepted='2026-09-01T10:00:00+00:00')
        got = latest_known([newer_period, older_period_later_filed],
                           dt.datetime(2027, 1, 1, tzinfo=UTC))
        assert got[1].accession_no == 'p1'

    def test_nothing_filed_yet_yields_nothing(self):
        assert latest_known([_f()], dt.datetime(2020, 1, 1, tzinfo=UTC)) == {}


class TestTheReportingLagIsAuditedNotCorrected:

    def test_the_measured_lag_is_reported(self):
        assert reporting_lag_days(dt.date(2026, 3, 28),
                                  dt.datetime(2026, 5, 1, tzinfo=UTC)) == 34

    def test_a_negative_lag_is_refused(self):
        """A filing accepted before the period it reports on is a data defect,
        not a fast filer, and must not be reported as a valid lag."""
        assert reporting_lag_days(dt.date(2026, 5, 1),
                                  dt.datetime(2026, 3, 28, tzinfo=UTC)) is None

    def test_a_missing_end_yields_none(self):
        assert reporting_lag_days(None, dt.datetime(2026, 5, 1, tzinfo=UTC)) is None
        assert reporting_lag_days(dt.date(2026, 3, 28), None) is None


class TestDegenerateRatiosStayOutOfRankings:
    """Loss-making companies have negative earnings and newly-listed ones
    report zero book value. In a 500-name universe these are the rule, not the
    exception, and 0.0 or infinity would place them at one END of a ranking -
    a view nobody expressed."""

    def test_a_zero_denominator_yields_none(self):
        assert safe_ratio(100.0, 0.0) is None

    def test_a_missing_input_yields_none(self):
        assert safe_ratio(None, 5.0) is None
        assert safe_ratio(5.0, None) is None

    def test_a_negative_denominator_is_a_real_ratio(self):
        """Negative equity is a fact about the company, not a defect."""
        assert safe_ratio(10.0, -5.0) == pytest.approx(-2.0)

    @settings(max_examples=200, deadline=None)
    @given(n=st.floats(allow_nan=True, allow_infinity=True),
           d=st.floats(allow_nan=True, allow_infinity=True))
    def test_the_result_is_finite_or_none(self, n, d):
        out = safe_ratio(n, d)
        assert out is None or (isinstance(out, float) and
                               out == out and abs(out) != float('inf'))


class TestConceptExtraction:

    def _payload(self, tag, value):
        return {'StatementsOfIncome': {
            tag: [{'value': value,
                   'period': {'startDate': '2025-12-28', 'endDate': '2026-03-28'}}]}}

    def test_a_concept_is_found_under_its_primary_tag(self):
        got = extract_concepts(self._payload(
            'RevenueFromContractWithCustomerExcludingAssessedTax', '80208000000'))
        assert got['revenue'] == pytest.approx(80_208_000_000.0)

    def test_an_alternative_tag_is_tried(self):
        """Filers disagree on tag names; a concept reported under a synonym
        must still be found or the universe silently shrinks to whoever uses
        the primary tag."""
        assert extract_concepts(self._payload('Revenues', '123'))['revenue'] \
            == pytest.approx(123.0)

    def test_an_unreported_concept_is_none_not_zero(self):
        """'Did not report' and 'reported zero' are different facts, and only
        one of them belongs in a ranking."""
        assert extract_concepts(self._payload('Revenues', '1'))['net_income'] is None

    def test_an_unparseable_value_is_none(self):
        assert extract_concepts(self._payload('Revenues', 'n/a'))['revenue'] is None

    def test_an_empty_payload_yields_all_none(self):
        got = extract_concepts({})
        assert got and all(v is None for v in got.values())


class TestThePolygonAdapterIsSeparate:
    """The first version routed Polygon objects through the XBRL extractor and
    produced 204 rows of correct timestamps attached to entirely empty values.
    The two schemas share nothing but intent - snake_case fields on nested
    objects versus XBRL tags in flat dicts - so one adapter serving both
    returns None for everything the moment either side moves."""

    class _V:
        def __init__(self, value): self.value = value

    class _Sec:
        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    class _Fin:
        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    def test_polygon_fields_are_read(self):
        from trader.data.fundamentals import extract_polygon
        fin = self._Fin(income_statement=self._Sec(revenues=self._V(111184000000.0)),
                        balance_sheet=self._Sec(assets=self._V(331000000.0)))
        got = extract_polygon(fin)
        assert got['revenue'] == pytest.approx(111184000000.0)
        assert got['total_assets'] == pytest.approx(331000000.0)

    def test_an_absent_section_yields_none_not_a_crash(self):
        from trader.data.fundamentals import extract_polygon
        got = extract_polygon(self._Fin())
        assert got and all(v is None for v in got.values())

    def test_a_none_valued_field_falls_through_to_the_alternative(self):
        """Filers differ on which equity field they populate; the first
        present one wins rather than the first listed."""
        from trader.data.fundamentals import extract_polygon
        fin = self._Fin(balance_sheet=self._Sec(
            equity=self._V(None), equity_attributable_to_parent=self._V(42.0)))
        assert extract_polygon(fin)['stockholders_equity'] == pytest.approx(42.0)

    def test_xbrl_extraction_is_unaffected_by_the_polygon_map(self):
        """The separation is the point: they must not share a lookup."""
        from trader.data.fundamentals import extract_concepts
        got = extract_concepts({'StatementsOfIncome': {
            'Revenues': [{'value': '5'}]}})
        assert got['revenue'] == pytest.approx(5.0)
