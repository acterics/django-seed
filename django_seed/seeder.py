import random

from django.db.models import ForeignKey, ManyToManyField, OneToOneField
from django.db.models.fields import AutoField

from django_seed.exceptions import SeederException, SeederOneToOneRelationException
from django_seed.guessers import NameGuesser, FieldTypeGuesser


one_to_one_indexes = {}
class ModelSeeder(object):
    def __init__(self, model):
        """
        :param model: Generator
        """
        self.model = model
        self.field_formatters = {}
        self.many_to_many_formatters = {}
        self.many_to_many_count_dict = {}
    
    @staticmethod
    def choice_unique(field, related_insertions):
        if field.name not in one_to_one_indexes:
            one_to_one_indexes[field.name] = []
        field_indexes = one_to_one_indexes[field.name]
        filtered_list = [i for i in related_insertions if i not in field_indexes]
        
        if not filtered_list:
            message = 'Field {} need more unique values of related model'.format(field)
            raise SeederOneToOneRelationException(message)
        pk = random.choice(filtered_list)
        one_to_one_indexes[field.name].append(pk)
        return pk



    @staticmethod
    def build_one_to_one_relation(field, related_model):
        def func(inserted):
            if related_model in inserted and inserted[related_model]:
                pk = ModelSeeder.choice_unique(field, inserted[related_model])
                return related_model.objects.get(pk=pk)
            elif not field.null:
                message = 'Field {} cannot be null'.format(field)
                raise SeederException(message)

        return func

    @staticmethod
    def build_one_to_many_relation(field, related_model):
        def func(inserted):
            if related_model in inserted and inserted[related_model]:
                pk = random.choice(inserted[related_model])
                return related_model.objects.get(pk=pk)
            elif not field.null:
                message = 'Field {} cannot be null'.format(field)
                raise SeederException(message)

        return func


    @staticmethod
    def build_many_to_many_relation(field, related_model, count=None):
        def func(obj, inserted):
            if related_model in inserted and inserted[related_model]:
                related_insetions = inserted[related_model]
                if count and count > 0:
                    actual_count = min(count, len(related_insetions))
                else:
                    actual_count = random.randint(1, len(related_insetions))
                
                ids = random.sample(related_insetions, actual_count)
                getattr(obj, field.attname).set(ids)
        return func

    def create_many_to_many_formatters(self):
        formatters = {}
        for field in self.model._meta.local_many_to_many:
            field_name = field.name
            if field_name in self.many_to_many_count_dict:
                count = self.many_to_many_count_dict[field_name]
            else:
                count = None
            formatters[field_name] = self.build_many_to_many_relation(field, field.related_model, count=count)

        return formatters


    def guess_field_formatters(self, faker):
        """
        Gets the formatter methods for each field using the guessers
        or related object fields
        :param faker: Faker factory object
        """
        formatters = {}
        name_guesser = NameGuesser(faker)
        field_type_guesser = FieldTypeGuesser(faker)

        for field in self.model._meta.fields:

            field_name = field.name

            if field.get_default(): 
                formatters[field_name] = field.get_default()
                continue
            
            if isinstance(field, OneToOneField):
                formatters[field_name] = self.build_one_to_one_relation(field, field.related_model)
                continue

        
            if isinstance(field, ForeignKey):
                formatters[field_name] = self.build_one_to_many_relation(field, field.related_model)
                continue

            if isinstance(field, AutoField):
                continue

            if not field.choices:
                formatter = name_guesser.guess_format(field_name)
                if formatter:
                    formatters[field_name] = formatter
                    continue

            formatter = field_type_guesser.guess_format(field)
            if formatter:
                formatters[field_name] = formatter
                continue
        
        


        return formatters

    def execute(self, using, inserted_entities):
        """
        Execute the stages entities to insert
        :param using:
        :param inserted_entities:
        """

        def format_field(format, inserted_entities):
            if callable(format):
                return format(inserted_entities)
            return format

        def format_many_to_many_field(obj, format, inserted_entities):
            if callable(format):
                format(obj, inserted_entities)
            else:
                message = "Many to many format must be callable"
                raise SeederException(message)
                

        def turn_off_auto_add(model):
            for field in model._meta.fields:
                if getattr(field, 'auto_now', False):
                    field.auto_now = False
                if getattr(field, 'auto_now_add', False):
                    field.auto_now_add = False

        manager = self.model.objects.db_manager(using=using)
        turn_off_auto_add(manager.model)



        create_dict = { 
            field: format_field(field_format, inserted_entities)
            for field, field_format in self.field_formatters.items()
        }



        obj = manager.create(**create_dict)
        for format in self.many_to_many_formatters.values():
            format_many_to_many_field(obj, format, inserted_entities)

        return obj.pk


class Seeder(object):
    def __init__(self, faker):
        """
        :param faker: Generator
        """
        self.faker = faker
        self.entities = {}
        self.quantities = {}
        self.orders = []

    def add_entity(self, model, number, customFieldFormatters=None, many_to_many_count_dict=None):
        """
        Add an order for the generation of $number records for $entity.

        :param model: mixed A Django Model classname,
        or a faker.orm.django.EntitySeeder instance
        :type model: Model
        :param number: int The number of entities to seed
        :type number: integer
        :param customFieldFormatters: optional dict with field as key and 
        callable as value
        :type customFieldFormatters: dict or None
        """
        if not isinstance(model, ModelSeeder):
            model = ModelSeeder(model)
        if many_to_many_count_dict:
            model.many_to_many_count_dict = many_to_many_count_dict
        
        model.field_formatters = model.guess_field_formatters(self.faker)
        model.many_to_many_formatters = model.create_many_to_many_formatters()
        
        if customFieldFormatters:
            model.field_formatters.update(customFieldFormatters)

        klass = model.model
        self.entities[klass] = model
        self.quantities[klass] = number
        self.orders.append(klass)

    def execute(self, using=None):
        """
        Populate the database using all the Entity classes previously added.

        :param using A Django database connection name
        :rtype: A list of the inserted PKs
        """
        if not using:
            using = self.get_connection()

        inserted_entities = {}
        for klass in self.orders:
            number = self.quantities[klass]
            if klass not in inserted_entities:
                inserted_entities[klass] = []
            for i in range(0, number):
                entity = self.entities[klass].execute(using, inserted_entities)
                inserted_entities[klass].append(entity)

        one_to_one_indexes.clear()
        return inserted_entities

    def get_connection(self):
        """
        use the first connection available
        :rtype: Connection
        """

        klass = self.entities.keys()
        if not klass:
            message = 'No classed found. Did you add entities to the Seeder?'
            raise SeederException(message)
        klass = list(klass)[0]

        return klass.objects._db
