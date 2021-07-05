import textwrap
from abc import ABC
from typing import List, Union


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

    def __str__(self) -> str:
        return textwrap.dedent(f'''
        name: {self.name}
        description: {self.description}
        required: {self.required}
        allowed_values: {self.allowed_values}
        default_value: {self.default_value}
        ''')


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


class PropertyException(Exception):
    pass


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


class PropertyGroup(dict):

    def set_property(self,
                     property_descriptor: PropertyDescriptor,
                     value):
        self[property_descriptor.name] = value

    def get_property(self,
                     property_descriptor: PropertyDescriptor,
                     default=None):
        property_name = property_descriptor.name
        if property_descriptor.required and property_name not in self:
            textwrap.dedent(f'''Property not found.
                            {property_descriptor}
                            ''')
        return self.get(property_name, default)


class PropertyGroups(dict):

    def set_property_group(self,
                           property_group_descriptor: PropertyGroupDescriptor,
                           property_group: Union[PropertyGroup, dict]):
        property_group_name = property_group_descriptor.group_name
        self[property_group_name] = property_group

    def get_property_group(
        self,
        property_group_descriptor: PropertyGroupDescriptor
    ) -> PropertyGroup:
        name = property_group_descriptor.group_name
        assert name in self, \
            f'Property group {name} not found!'

        return self[name]


def get_boolean_value(string_value: str, default: bool) -> bool:
    if string_value.lower() in ['yes', 'true', 'y']:
        return True
    elif string_value.lower() in ['no', 'false', 'n']:
        return False
    else:
        return default
