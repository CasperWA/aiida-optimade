"""Make sure response_fields is handled correctly"""
# pylint: disable=redefined-outer-name
import pytest


@pytest.fixture
def check_required_fields_response(get_good_response):
    """Fixture to check "good" `required_fields` response"""
    from aiida_optimade import mappers

    get_mapper = {
        "structures": mappers.StructureMapper,
    }

    def _check_required_fields_response(
        endpoint: str,
        known_unused_fields: set,
        expected_fields: set,
    ):
        expected_fields |= (
            get_mapper.get(
                endpoint, mappers.ResourceMapper
            ).get_required_fields() - known_unused_fields
        )
        expected_fields.add("attributes")
        request = f"/{endpoint}?response_fields={','.join(expected_fields)}"

        response = get_good_response(request)

        response_fields = set()
        for entry in response["data"]:
            response_fields.update(set(entry.keys()))
            response_fields.update(set(entry["attributes"].keys()))
        assert sorted(expected_fields) == sorted(response_fields)

    return _check_required_fields_response


def test_required_fields_links(check_required_fields_response):
    """Certain fields are REQUIRED, no matter the value of `response_fields`"""
    endpoint = "links"
    illegal_top_level_field = "relationships"
    non_used_top_level_fields = {"links"}
    non_used_top_level_fields.add(illegal_top_level_field)
    expected_fields = {"homepage", "base_url", "link_type"}
    check_required_fields_response(endpoint, non_used_top_level_fields, expected_fields)


@pytest.mark.skip("References has not yet been implemented")
def test_required_fields_references(check_required_fields_response):
    """Certain fields are REQUIRED, no matter the value of `response_fields`"""
    endpoint = "references"
    non_used_top_level_fields = {"links", "relationships"}
    expected_fields = {"year", "journal"}
    check_required_fields_response(endpoint, non_used_top_level_fields, expected_fields)


def test_required_fields_structures(check_required_fields_response):
    """Certain fields are REQUIRED, no matter the value of `response_fields`"""
    endpoint = "structures"
    non_used_top_level_fields = {"links"}
    expected_fields = {"elements", "nelements"}
    check_required_fields_response(endpoint, non_used_top_level_fields, expected_fields)
