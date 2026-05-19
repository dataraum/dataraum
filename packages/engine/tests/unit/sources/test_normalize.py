"""Tests for column name normalization."""

from dataraum.sources.base import normalize_column_name


class TestNormalizeColumnName:
    """Tests for normalize_column_name()."""

    def test_basic_spaces_to_underscores(self):
        assert normalize_column_name("Business Id") == "business_id"

    def test_simple_lowercase(self):
        assert normalize_column_name("Amount") == "amount"

    def test_mixed_case_with_spaces(self):
        assert normalize_column_name("Transaction date") == "transaction_date"

    def test_multiple_spaces(self):
        assert normalize_column_name("Customer  full  name") == "customer_full_name"

    def test_diacritics_stripped(self):
        assert normalize_column_name("Gebührennummer") == "gebuhrennummer"

    def test_diacritics_accented_e(self):
        assert normalize_column_name("café") == "cafe"

    def test_ampersand_removed(self):
        assert normalize_column_name("Profit & Loss") == "profit_loss"

    def test_hyphen_removed(self):
        assert normalize_column_name("first-name") == "firstname"

    def test_comma_removed(self):
        assert normalize_column_name("city, state") == "city_state"

    def test_slash_removed(self):
        assert normalize_column_name("A/R paid") == "ar_paid"

    def test_leading_trailing_whitespace(self):
        assert normalize_column_name("  Amount  ") == "amount"

    def test_leading_digit_prefixed(self):
        assert normalize_column_name("1st Quarter") == "c_1st_quarter"

    def test_empty_after_normalization(self):
        assert normalize_column_name("---", position=3) == "column_3"

    def test_empty_string(self):
        assert normalize_column_name("", position=0) == "column_0"

    def test_underscores_collapsed(self):
        assert normalize_column_name("a__b___c") == "a_b_c"

    def test_leading_trailing_underscores_stripped(self):
        assert normalize_column_name("_name_") == "name"

    def test_product_service_underscore(self):
        assert normalize_column_name("Product_Service") == "product_service"

    def test_product_service_type(self):
        assert normalize_column_name("Product_Service_Type") == "product_service_type"

    def test_billing_zip_code(self):
        assert normalize_column_name("Billing ZIP code") == "billing_zip_code"

    def test_ar_paid(self):
        assert normalize_column_name("A/R paid") == "ar_paid"

    def test_ap_paid(self):
        assert normalize_column_name("A/P paid") == "ap_paid"

    def test_credit_card(self):
        assert normalize_column_name("Credit card") == "credit_card"


class TestCollisionHandling:
    """Tests for handling duplicate normalized names."""

    def test_duplicate_names_get_suffix(self):
        """Simulate the collision logic used in the loader."""
        headers = ["ID", "id", "Id"]
        seen: dict[str, int] = {}
        results = []
        for h in headers:
            normalized = normalize_column_name(h)
            if normalized in seen:
                seen[normalized] += 1
                normalized = f"{normalized}_{seen[normalized]}"
            else:
                seen[normalized] = 1
            results.append(normalized)

        assert results == ["id", "id_2", "id_3"]

    def test_no_collision_different_names(self):
        headers = ["name", "email"]
        seen: dict[str, int] = {}
        results = []
        for h in headers:
            normalized = normalize_column_name(h)
            if normalized in seen:
                seen[normalized] += 1
                normalized = f"{normalized}_{seen[normalized]}"
            else:
                seen[normalized] = 1
            results.append(normalized)

        assert results == ["name", "email"]
