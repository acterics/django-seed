import random

from django.db.models import ForeignKey, ManyToManyField, OneToOneField
from django.db.models.fields import AutoField

from django_seed.exceptions import SeederException, SeederOneToOneRelationException
from django_seed.guessers import NameGuesser, FieldTypeGuesser


class ModelSeeder(object):
    def __init__(self, model):
        """
        :param model: Generator
        """
        self.model = model
        self.field_formatters = {}

    one_to_one_indexes = {}

    @staticmethod
    def choice_unique(field, related_insetions):
        if not one_to_one_indexes[field]:
            one_to_one_indexes[field] = []
        field_indexes = one_to_one_indexes[field]
        filtered_list = [i for i in related_insertions if i not in field_indexes]
        if not filtered_list:
            message = 'Field {} need more unique values of related model'.format(field)
            raise SeederOneToOneRelationException(message)
        pk = random.choice(filtered_list)
        field_indexes.append(pk)
        return pk



    @staticmethod
    def build_one_to_one_relation(field, related_model):
        def func(inserted):
            if related_model in inserted and inserted[related_model]:
                pk = choice_unique(field, inserted[related_model])
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


    def build_many_to_many_relation(field, related_model):
        def func(inserted):
            if related_model in inserted and inserted[related_model]:
                related_insetions = inserted[related_model]
                ids = random.sample(related_insetions, random.randint(1, related_insetions.size))
                return related_model.objects.filter(id__in=type_ids)
            elif not field.null:
                message = 'Field {} cannot be null'.format(field)
                raise SeederException(message)

        return func

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
            
            if isinstance(field, ForeignKey):
                formatters[field_name] = self.build_one_to_many_relation(field, field.related_model)
                continue

            if isinstance(field, OneToOneField):
                formatters[field_name] = self.build_one_to_one_relation(field, field.related_model)
                continue
            
            if isinstance(field, ManyToManyField):
                formatters[field_name] = self.build_many_to_many_relation(field, field.related_model)
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

        def turn_off_auto_add(model):
            for field in model._meta.fields:
                if getattr(field, 'auto_now', False):
                    field.auto_now = False
                if getattr(field, 'auto_now_add', False):
                    field.auto_now_add = False

        manager = self.model.objects.db_manager(using=using)
        turn_off_auto_add(manager.model)

        obj = manager.create(**{
            field: format_field(field_format, inserted_entities)
            for field, field_format in self.field_formatters.items()
        })

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

    def add_entity(self, model, number, customFieldFormatters=None):
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

        model.field_formatters = model.guess_field_formatters(self.faker)
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
