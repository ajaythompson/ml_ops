from abc import ABC
from typing import List


class PropertyDescriptor:

    def __init__(self,
                 name,
                 description,
                 required,
                 allowed_values,
                 default_value,
                 validators) -> None:
        self.name = name
        self.description = description
        self.required = required
        self.allowed_values = allowed_values
        self.default_value = default_value
        self.validators = validators


class PropertyDescriptorBuilder:

    def __init__(self):
        self.__name = ''
        self.__description = ''
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
            required=self.__required,
            allowed_values=self.__allowed_values,
            default_value=self.__default_value,
            validators=self.__validators
        )


class PropertyGroupDescriptor(ABC):

    def __init__(self, group_name,
                 prop_descriptors: List[PropertyDescriptor] = []):
        self.group_name = group_name
        self.prop_descriptors = {prop_descriptor.name: prop_descriptor
                                 for prop_descriptor in prop_descriptors}

    def get_property(self, prop_descriptor: PropertyDescriptor):
        property_name = prop_descriptor.name
        assert property_name in self.prop_descriptors, \
            f'Property {prop_descriptor} not found in the' \
            ' property group {self.group_name}.'
        return self.prop_descriptors.get(property_name)
