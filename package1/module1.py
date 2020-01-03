import datetime
import pandas as pd

now = datetime.datetime.now()


def title_column(df, column):
    df[column] = df[column].str.title()
    return df[column]


def gender_f(the_row):
    if the_row == "M":
        return "Male"
    elif the_row == "F":
        return "Female"
    else:
        return the_row


def age_f(the_row):
    the_row = int(the_row)  # añadido!!!!
    if the_row >= 1000:
        return now.year - the_row
    else:
        return the_row


def birthdate_f(the_row):
    if the_row > 0:
        date = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=the_row / 1000)
        return date.strftime('%Y')
    elif the_row < 0:
        date = datetime.datetime(1970, 1, 1) - datetime.timedelta(seconds=-the_row / 1000)
        return date.strftime('%Y')
    else:
        return 0


def age_none_f(age, birthDate):
    if age == 0:
        return (birthDate - 1)
    else:
        return age


def gender_none_f(Gender, gender):
    if Gender in (None, '', 'None'):
        return gender
    else:
        return Gender


def country_none_f(Country, countryOfCitizenship):
    if Country in (None, '', 'None'):
        return countryOfCitizenship
    elif Country == 'USA':
        return 'United States'
    else:
        return Country


def country_none2_f(Country, Citizenship):
    if pd.isnull(Country):
        return Citizenship
    elif Country == 'USA':
        return 'United States'
    else:
        return Country
