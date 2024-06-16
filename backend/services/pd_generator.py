from mimesis import Person
from mimesis.locales import Locale
from mimesis import Datetime
from mimesis import Generic
from mimesis.enums import Gender
import random


class PersonalDataGenerator:
    def __init__(self, consistency: bool = False):
        self.person = Person(Locale.RU)
        self.datetime = Datetime(Locale.RU)
        self.generic = Generic(Locale.RU)
        self._internal_data = {}
        self.consistency = consistency

    def generate(self, s: str, ent_type: str):
        if self.consistency:
            if self._internal_data.get(s, {}).get(ent_type):
                return self._internal_data[s][ent_type]
            elif self._internal_data.get(s):
                self._internal_data[s][ent_type] = self._generate(ent_type)
                return self._internal_data[s][ent_type]
            else:
                self._internal_data[s] = {ent_type: self._generate(ent_type)}
                return self._internal_data[s][ent_type]

    def _generate(self, ent_type: str):
        if ent_type == 'PER':
            random_gender = random.choice([Gender.MALE, Gender.FEMALE])
            return self.person.name(gender=random_gender)
        elif ent_type == 'DATE':
            return self.datetime.formatted_date()
        elif ent_type == 'CONTACTS':
            return self.person.email()
        elif ent_type == 'ORG':
            return self.generic.finance.company()
        elif ent_type == 'LOC':
            return self.generic.address.city()
        elif ent_type == 'SENSITIVE':
            return self.generic.numeric.integer_number(start=10000000, end=99999999)
        elif ent_type == 'EMAIL':
            return self.generic.person.email()
        elif ent_type == 'PHONE':
            return self.generic.person.phone_number(mask='+7(9##)#######')
        elif ent_type == 'URL':
            return self.generic.internet.hostname()
        else:
            return self.generic.text.word()


if __name__ == "__main__":
    pd_gen = PersonalDataGenerator(consistency=True)

    s1 = "PER1"
    s2 = "PER2"

    print(f"\n\033[096mGenerating for {s1}...\033[0m")
    for ent in ['PER', 'DATE', 'CONTACTS', 'ORG', 'LOC', 'SENSITIVE', 'EMAIL', 'PHONE', 'URL']:
        print(pd_gen.generate(s1, ent))

    print(f"\n\033[096mGenerating for {s2}...\033[0m")
    for ent in ['PER', 'DATE', 'CONTACTS', 'ORG', 'LOC', 'SENSITIVE']:
        print(pd_gen.generate(s2, ent))

    print(f"\n\033[096mGenerating for {s2}...\033[0m")
    for ent in ['PER', 'DATE', 'CONTACTS', 'ORG', 'LOC', 'SENSITIVE']:
        print(pd_gen.generate(s2, ent))
