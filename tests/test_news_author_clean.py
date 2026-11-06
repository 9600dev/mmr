from trader.sdk import _clean_author


def test_placeholder_authors_become_empty():
    for v in ('Na', 'N/A', 'Unknown', 'none', None, '', '  ', 'nan'):
        assert _clean_author(v) == ''


def test_real_author_kept():
    assert _clean_author(' John Ballard ') == 'John Ballard'
    assert _clean_author('Zacks.Com') == 'Zacks.Com'
