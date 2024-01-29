import re
import pandas as pd
import xml.etree.ElementTree as et


def convert_formula(formula):
    # Replace ZN with IFNULL
    formula = formula.replace("ZN(", "IFNULL(")
    formula = formula.replace("zn(", "IFNULL(")

    # Remove comments
    formula = re.sub(r'//.*?(\n|$)', '', formula)

    # Replace Tableau-specific functions
    formula = formula.replace("[", "").replace("]", "").replace("IFNULL(", "ISNULL(")

    return formula


def extract_tableau_calculated_feild(tableau_workbook_path):
    try:
        tree = et.parse(tableau_workbook_path)
        root = tree.getroot()
        calc_fields = []

        for column in root.findall('.//datasource-dependencies/column[calculation]'):
            calc = column.find('.//calculation')
            formula = calc.get('formula')

            col = {
                'column_name': column.get('caption'),
                'formula': formula
            }

            calc_fields.append(col)

        if calc_fields:
            print(calc_fields[0])  # Print the first calculated field for demonstration

        return calc_fields

    except et.ParseError as e:
        print(f"Error parsing XML: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


converted_formulas = []

data = extract_tableau_calculated_feild('./ExtractFiles/Climate Change.twb')

for formula_dict in data:
    column_name = formula_dict.get('column_name')
    formula = formula_dict.get('formula')

    if formula:
        converted_formula = convert_formula(formula)
        converted_formulas.append({'column_name': column_name, 'formula': converted_formula})
converted_df=pd.DataFrame(converted_formulas)
converted_df.to_csv('converted Dax expn.csv')