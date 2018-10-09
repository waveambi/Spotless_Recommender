import unittest
import sys
sys.path.append("../helpers/")
import os
import json
import helper



class TestHelpersMethods(unittest.TestCase):
    """
    class that contains unit tests for methods from helpers.py
    """

    def setUp(self):
        """
        initialization of test suites
        """
        pass



    def tearDown(self):
        """
        cleanup after test suites
        """
        pass


    def test_trim_zipcode(self):
        # test if correctly trim postal code
        raw_code = 10025-9090
        trimmed_code = 10025
        self.assertItemsEqual(trimmed_code,
                helper.trim_zipcode(raw_code),
                'incorrect trimmed code returned')



    def test_format_address(self):
        # test on get formatted address
        address = "501 w 113th St 1080 Amsterdam"
        ss = "501 w 113th"
        self.assertItemsEqual(ss,
                helper.format_address(address),
                "incorrect format address returned")



    def test_format_name(self):
        # test if getting unique part of name correctly
        name = "Strokos Restaurant, -- St Luke Hospital"
        formatted_name = "strokos"
        self.assertItemsEqual(formatted_name,
                helper.format_name(name),
                'incorrect format name returned')



    def test_fuzzy_match(self):
        # test fuzz match results
        s1 = "diginn"
        s2 = "digin"
        similarity = 0.8
        self.assertItemsEqual(similarity,
                helper.fuzzy_match(s1, s2),
                "can't match similarity")


    def test_calculate_score(self):
        # test score calculation results
        x, y, z = 1, 1, 1
        score = 17/30
        self.assertItemsEqual(score,
                helper.calculate_score(x, y, z),
                "errors in score calculation")


    def test_parse_config(self):
        # test if correctly parses the config file
        conf = {"field1": "val1",
                "field2": {"subfield1": 2, "subfield2": "3"}}

        with patch("__builtin__.open",
                   mock_open(read_data=json.dumps(conf))) as mock_file:

            self.assertEqual(conf,
                    helper.parse_config(mock_file),
                    "fail to properly read config from file")



    def test_replace_envvars_with_vals(self):
        # test if correctly parses environmental variables
        dic1 = {"field1": "$TESTENVVAR:15,$TESTENVVAR",
                "field2": {"sf1": 1, "sf2": "$TESTENVVAR"}}
        dic2 = {"field1": "test123:15,test123",
                "field2": {"sf1": 1, "sf2": "test123"}}

        with patch.dict(os.environ, {'TESTENVVAR': 'test123'}, clear=True):

            self.assertEqual(dic2,
                    helper.replace_envvars_with_vals(dic1),
                    "did not correctly parse environmental variables")



if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestHelpersMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)