import pytest

from src.titles.find_brands import (
    make_annotations,
    process,
    sub_all,
    sub_if,
)


class TestSubIf(object):
    def test_no_match(self):
        string = "Hello, world"
        pattern = "Hey"
        repl = "Hi"
        expected = string
        assert sub_if(pattern, repl, string) == expected

    def test_substitutes(self):
        string = "Hello, world"
        pattern = "Hello"
        repl = "Hi"
        expected = "Hi, world"
        assert sub_if(pattern, repl, string) == expected

    def test_pattern_group(self):
        string = "Hello, world"
        pattern = r"^(\w+),"
        repl = r"\1!"
        expected = "Hello! world"
        assert sub_if(pattern, repl, string) == expected

class TestSubAll(object):
    def test_no_matches(self):
        string = "aabcde"
        pattern = "f"
        repl = "F"
        expected = string
        assert sub_all(pattern, repl, string) == expected

    def test_one_match(self):
        string = "aabcde"
        pattern = "e"
        repl = "E"
        expected = "aabcdE"
        assert sub_all(pattern, repl, string) == expected

    def test_multiple_matches(self):
        string = "aabcde"
        pattern = "a"
        repl = "A"
        expected = "AAbcde"
        assert sub_all(pattern, repl, string) == expected

class TestProcess(object):
    def test_unescapes_html(self):
        title = "Priest &gt; Maiden"
        expected = "Priest > Maiden"
        assert process(title) == expected

    def test_replaces_ampersand(self):
        title = "A&B"
        expected = "A and B"
        assert process(title) == expected

    def test_removes_possessive(self):
        title = "Mel's Diner"
        expected = "Mel Diner"
        assert process(title) == expected

    @pytest.mark.parametrize(
        "title, expected", [
            ("R.M. Williams", "RM Williams"),
            ("RM. Williams", "RM Williams"),
            ("R.M Williams", "RM Williams"),
            ("Sentences. Are compromised", "Sentences Are compromised"),
        ])
    def test_removes_dots(self, title, expected):
        assert process(title) == expected

class TestMakeAnnotations(object):
    def test_makes_all_annotations(self):
        brands = ("Alden", "JCrew")
        title = "[Review] Alden x JCrew Indy boots"
        s_id = "1"

        annotations = make_annotations(title, brands, s_id)

        assert len(annotations) == 2
        assert all(a.submission_id == s_id for a in annotations)
        assert annotations[0].brand == "Alden"
        assert annotations[1].brand == "JCrew"
