from typing import List


class PropertyDescriptor:

    def __init__(self,
                 name,
                 description,
                 property_group,
                 required,
                 allowed_values,
                 default_value,
                 validators) -> None:
        self.name = name
        self.description = description
        self.property_group = property_group
        self.required = required
        self.allowed_values = allowed_values
        self.default_value = default_value
        self.validators = validators


class PropertyDescriptorBuilder:

    def __init__(self):
        self.__name = ''
        self.__description = ''
        self.__property_group = 'default'
        self.__required = False
        self.__allowed_values = []
        self.__default_value = None
        self.__validators = []

    def name(self, name):
        self.__name = name
        return self

    def description(self, description):
        self.__description = description
        return self

    def property_group(self, property_group):
        self.__property_group = property_group
        return self

    def required(self, required):
        self.__required = required
        return self

    def allowed_values(self, allowed_values):
        self.__required = allowed_values
        return self

    def default_value(self, default_value):
        self.__default_value = default_value
        return self

    def validators(self, validators):
        self.__validators = validators
        return self

    def build(self):
        return PropertyDescriptor(
            name=self.__name,
            description=self.__description,
            property_group=self.__property_group,
            required=self.__required,
            allowed_values=self.__allowed_values,
            default_value=self.__default_value,
            validators=self.__validators
        )


class PropertyGroup:

    def __init__(self, group_name, prop_descriptors: List[PropertyDescriptor]):
        self.group_name = group_name
        self.prop_descriptors = {x.name: x for x in prop_descriptors}
