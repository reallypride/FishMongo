import pymongo
from pymongo.cursor import Cursor
from . import signals
import datetime
import settings
from importlib import import_module
from django.utils.dateparse import parse_datetime
from bson.objectid import ObjectId
from django.core.cache import cache

class Field(object):
    def __init__(self, name=None, default=None):
        self.name = name
        self.default = default
        
    def to_value(self, value):
        return value
    
    def get_default(self):
        return self.default
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.default
    
class ForeignRelated(object):
    def __init__(self, rel, name, rel_field_name):
        self.rel = rel
        self.name = name
        self.cache_name = '_cache_%s' % self.name
        self.rel_field_name = rel_field_name
        
    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return getattr(instance, self.cache_name)
        except:
            spec = {self.rel_field_name:instance}
            rel_objs = self.rel.objects.filter(**spec)
            setattr(instance, self.cache_name, rel_objs)
            return rel_objs

class ForeignKey(object):
    def __init__(self, rel, name=None, default=None, related_name=None, null=False):
        self.rel = rel
        self.name = name
        self.default = default
        self.related_name = related_name
        self.null = null

    def to_value(self, value):
        obj = self.rel.objects.get(id=value)
        return value.id
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return getattr(instance, self.cache_name)
        except:
            rel_id = getattr(instance, self.rel_key, None)
            if rel_id is None:
                return None
            rel_obj = self.rel.objects.get(id=rel_id)
            setattr(instance, self.cache_name, rel_obj)
            return rel_obj
    
    def __set__(self, instance, value):
        if instance is None:
            raise Exception('%s must be accessed via instance' % self.rel._meta.object_name)
        if value is None:
            if self.null:
                setattr(instance, self.cache_name, value)
                setattr(instance, self.rel_key, 0)
            else:
                raise Exception('%s must not None' % self.rel._meta.object_name)
        else:
            if not isinstance(value, self.rel):
                try:
                    id = int(value)
                    value = self.rel.objects.get(id=id)
                except:
                    raise Exception('%s class type error' % self.rel._meta.object_name)
            setattr(instance, self.cache_name, value)
            setattr(instance, self.rel_key, value.id)
        
class GenerForeignKey(object):
    def __init__(self, type_field="content_type", pk_field="object_id", null=True):
        self.type_field = type_field
        self.pk_field = pk_field
        self.null = null
        
    def set_name(self, name):
        self.name = name
        self.cache_name = '_cache_%s' % self.name
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return getattr(instance, self.cache_name)
        except:
            type = getattr(instance, self.type_field)
            pk = getattr(instance, self.pk_field)
            if type and pk:
                mn, cls_name = type.split('.', 1)
                tmcls = instance.__class__
                modulename = tmcls.__module__
                mname = '%s.%s' % (modulename.rsplit('.', 1)[0], mn)
                module = import_module(mname)
                cls = getattr(module, cls_name)
                obj = cls.objects.get(pk=pk)
                return obj
            else:
                return None
        
    def __set__(self, instance, value):
        if instance is None:
            raise Exception('%s must be accessed via instance' % self.rel._meta.object_name)
        if not isinstance(value, Document) and value is not None:
            raise Exception('%s class type error' % self.rel._meta.object_name)
        if value is None and not self.null:
            raise Exception('%s class not None' % self.rel._meta.object_name)
        if value:
            setattr(instance, self.cache_name, value)
            cls = value.__class__
            mname = cls.__module__.rsplit('.', 1)[1]
            type = '%s.%s' % (mname, cls.__name__)
            pk = value.pk
            setattr(instance, self.type_field, type)
            setattr(instance, self.pk_field, pk)
    
class IntegerField(Field):
    def __init__(self, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = 0
        super(IntegerField, self).__init__(**kwargs)
        
    def to_value(self, value):
        try:
            return int(value)
        except:
            return 0
    
    def get_default(self):
        return self.default
    
class BooleanField(Field):
    def __init__(self, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = False
        super(BooleanField, self).__init__(**kwargs)
        
    def to_value(self, value):
        return bool(value)
    
    def get_default(self):
        return self.default or False
    
class DatetimeField(Field):
    def __init__(self, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = datetime.datetime.now()
        super(DatetimeField, self).__init__(**kwargs)
        
    def to_value(self, value):
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        if isinstance(value, unicode):
            value = str(value)
        if isinstance(value, str):
            return parse_datetime(value)
    
class DateField(Field):
    def __init__(self, **kwargs):
        if 'default' not in kwargs:
            today = datetime.date.today()
            dt = datetime.datetime(today.year, today.month, today.day)
            kwargs['default'] = dt
        super(DateField, self).__init__(**kwargs)
        
    def to_value(self, value):
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        if isinstance(value, unicode):
            value = str(value)
        if isinstance(value, str):
            return parse_datetime(value)
        
class ArrayField(Field):
    def __init__(self, **kwargs):
        if 'sep' in kwargs:
            self.sep = kwargs['sep']
        else:
            self.sep = ' '
        super(ArrayField, self).__init__(**kwargs)
        
    def to_value(self, value):
        if isinstance(value, str):
            return value.split(self.sep)
        if isinstance(value, unicode):
            return value.split(self.sep)
        return value
        
    def get_default(self):
        return self.default
    
class DictField(Field):
    def __init__(self, fields, **kwargs):
        self.fields = fields
        super(DictField, self).__init__(**kwargs)
    
    def to_value(self, value):
        for k, v in value.items():
            if k in self.fields:
                value[k] = self.fields[k].to_value(v)
        return value
    
    def get_default(self):
        data = {}
        for k, v in self.fields.items():
            data[k] = v.get_default()
        return data
    
class ManyKey(object):
    def __init__(self, rel, name=None, default=None):
        self.rel = rel
        self.default = default
        self.name = name

    def to_value(self, value):
        rel_ids = getattr(instance, self.rel_key, None)
        cursor = self.rel.objects.filter(id__in=rel_ids)
        objs = list(cursor)
        return objs
    
    def to_ids(self, value):
        ids = []
        for item in value:
            if isinstance(item, Document):
                ids.append(item.id)
            else:
                ids.append(item)
        return ids
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return getattr(instance, self.cache_name)
        except:
            rel_ids = instance.__dict__[self.rel_key]
            cursor = self.rel.objects.filter(id__in=rel_ids)
            objs = list(cursor)
            setattr(instance, self.cache_name, objs)
            return objs
    
    def __set__(self, instance, value):
        if instance is None:
            raise Exception('%s must be accessed via instance' % self.rel._meta.object_name)
        if value is None:
            setattr(instance, self.cache_name, value)
            setattr(instance, self.rel_key, [])
        else:
            if not isinstance(value, list):
                raise Exception('%s class type error' % self.rel._meta.object_name)
            ids = self.to_ids(value)
            instance.__dict__[self.rel_key] = ids

class MCursor(Cursor):
    def __init__(self, cursor, document):
        self.__dict__ = cursor.__dict__
        self.document = document
    
    def set_document(self, document):
        self.document = document
    
    def __getitem__(self, index):
        item = super(MCursor, self).__getitem__(index)
        if isinstance(item, Cursor):
            return list(item)
        return self._data2obj(item)
    
    def next(self):
        item = super(MCursor, self).next()
        return self._data2obj(item)
    
    def _data2obj(self, data):
        if not data:
            return None
        obj = self.document(**data)
        if getattr(obj, 'id', None) is None:
            name = self.document._meta.collection_name
            obj.id = AutoID.get_id(name)
            obj.save()
        return obj
    
    def order_by(self, *args):
        sort = []
        for arg in args:
            key = arg
            direction = pymongo.ASCENDING
            if key.find('-') == 0:
                key = key[1:]
                direction = pymongo.DESCENDING
            sort.append((key, direction))
        return super(MCursor, self).sort(sort)
    
    def __len__(self):
        return self.count()
    
    def __del__(self):
        self.close_client()
    
    def close_client(self):
        self.collection.database.connection.close()
    
    def all(self):
        return self

class Manager(object):
    opts = ('in', 'nin', 'lt', 'gt', 'ne', 'lte', 'gte', 'regex', 'exists')
    
    def __init__(self, document=None):
        self.document = document
        self._db = None
        self._wdb = None
        self._collection = None
        self._wcollection = None
        self._xcollection = None
        self._client = None
        self._wclient = None
        
    def set_document(self, document):
        self.document = document
        
    def get_db(self):
        if not self._db:
            dbconf = settings.MONGODBS['default']
            dbname = dbconf['NAME']
            dbhost = dbconf.get('HOST') or 'localhost'
            dbport = dbconf.get('PORT') or 27017
            self._client = pymongo.MongoClient(dbhost, dbport, max_pool_size=400)
            self._db = self._client[dbname]
        return self._db
    
    def get_wdb(self):
        if not self._wdb:
            dbconf = settings.MONGODBS.get('write')
            if dbconf:
                dbname = dbconf['NAME']
                dbhost = dbconf.get('HOST') or 'localhost'
                dbport = dbconf.get('PORT') or 27017
                self._wclient = pymongo.MongoClient(dbhost, dbport, max_pool_size=200)
                self._wdb = self._wclient[dbname]
            else:
                self._wdb = self.get_db()
        return self._wdb
    
    def using(self, name):
        client = pymongo.MongoClient()
        self._db = client[name]
        return self

    def collection(self):
        if not self._collection:
            db = self.get_db()
            collection_name = self.document._meta.collection_name
            self._collection = db[collection_name]
        return self._collection
    
    def wcollection(self):
        if not self._wcollection:
            db = self.get_wdb()
            collection_name = self.document._meta.collection_name
            self._wcollection = db[collection_name]
        return self._wcollection
    
    def __del__(self):
        self.close_client()
        self.close_wclient()
    
    def close_client(self):
        if self._client:
            self._client.close()
    
    def close_wclient(self):
        if self._wclient:
            self._wclient.close()
    
    def ensure_index(self, keylist, cache_for=300, **kwargs):
        self.wcollection().ensure_index(keylist, cache_for, **kwargs)
    
    def get(self, **kwargs):
        if 'sort' in kwargs:
            sort = kwargs['sort']
            del kwargs['sort']
        else:
            sort = None
        filter = self._clean_kwargs(kwargs)
        data = self.collection().find_one(filter, sort=sort)
        obj = self._data2obj(data)
        return obj 
    
    def create(self, **kwargs):
        obj = self._data2obj(kwargs)
        obj.save()
        return obj
    
    def save(self, **kwargs):
        d = self._clean_kwargs(kwargs)
        result = self.wcollection().save(d)
        return result
    
    def get_or_create(self, **kwargs):
        obj = self.get(**kwargs)
        created = False
        if not obj:
            obj = self.create(**kwargs)
            created = True
        return (obj, created)
    
    def filter(self, **kwargs):
        spec = self._clean_kwargs(kwargs, is_find=True)
        cursor = self.collection().find(spec)
        return MCursor(cursor, self.document)
    
    def find(self, spec):
        cursor = self.collection().find(spec)
        return MCursor(cursor, self.document)
    
    def update(self, spec, **kwargs):
        upsert = False
        if 'upsert' in kwargs:
            upsert = kwargs['upsert']
            del kwargs['upsert']
        multi = False
        if 'multi' in kwargs:
            multi = kwargs['multi']
            del kwargs['multi']
        doc = self._parse_kwargs(kwargs)
        spec = self._clean_kwargs(spec)
        if '$inc' not in doc and '$set' not in doc:
            raise Exception('%s update doc error' % doc)
        return self.wcollection().update(spec, doc, upsert=upsert, multi=multi)
    
    def remove(self, **kwargs):
        spec = self._clean_kwargs(kwargs)
        if not spec:
            raise Exception('%s remove spec error' % spec)
        return self.wcollection().remove(spec)
    
    def delete(self, **kwargs):
        mcursor = self.filter(**kwargs)
        for obj in mcursor:
            obj.delete()
    
    def id2_id(self, **kwargs):
        mcursor = self.filter(**kwargs)
        for obj in mcursor:
            obj.id2_id(**kwargs)
        
    def _data2obj(self, data):
        if not data:
            return None
        obj = self.document(**data)
        return obj
    
    def all(self):
        cursor = self.collection().find()
        mcursor = MCursor(cursor, self.document)
        return mcursor
    
    def count(self):
        return self.collection().count()
    
    def _parse_kwargs(self, kwargs):
        doc = {}
        inc = {}
        set = {}
        for k, v in kwargs.items():
            if isinstance(v, Document):
                t = self._parse_relate_field(k, v)
                if isinstance(t, tuple):
                    set[t[0]] = t[1]
                if isinstance(t, list):
                    for a, b in t:
                        set[a] = b
            elif k.find('__') > 0:
                tmps = k.split('__')
                if tmps[-1] == 'inc':
                    tmp = '.'.join(tmps[0:-1])
                    kk = '__'.join(tmps[0:-1])
                    inc[tmp] = self._parse_value(kk, v)
                else:
                    tmp = '.'.join(tmps)
                    set[tmp] = self._parse_value(k, v)
            else:
                set[k] = self._parse_value(k, v)
        if inc:
            doc['$inc'] = inc
        if set:
            doc['$set'] = set
        return doc
        
    def _clean_kwargs(self, kwargs, is_find=False):
        params = {}
        for k, v in kwargs.items():
            t = self._clean_arg(k, v, is_find)
            if isinstance(t, list):
                for a in t:
                    params[a[0]] = a[1]
            else:
                if t is not None:
                    key = t[0]
                    val = t[1]
                    if key in params:
                        params[key].update(val)
                    else:
                        params[key] = val
        return params
        
    def _clean_arg(self, key, value, is_find=False):
        if key.find('_') == 0 and key != '_id':
            return None
        elif isinstance(value, Document):
            return self._parse_relate_field(key, value)
        elif key == 'pk':
            try:
                key = 'id'
                value = int(value)
            except:
                key = '_id'
                value = ObjectId(value)
        elif key == 'id':
            try:
                value = int(value)
            except:
                key = '_id'
                value = ObjectId(value)
        elif key.find('__') > 0:
            tmps = key.split('__')
            l = tmps[-1]
            if l in self.opts:
                key = '.'.join(tmps[0:-1])
                if l in ['in', 'nin', 'regex']:
                    kk = '__'.join(tmps)
                    if l in ['in', 'nin']:
                        items = [self._parse_value(key, a) for a in value]
                        value = {'$'+l:items}
                    else:
                        value = {'$'+l:self._parse_value(kk, value)}
                else:
                    kk = '__'.join(tmps[0:-1])
                    value = {'$'+l:self._parse_value(kk, value)}
            else:
                value = self._parse_value(key, value)
                key = '.'.join(tmps)
        else:
            value = self._parse_value(key, value, is_find)
        return (key, value)
    
    def _parse_relate_field(self, key, value):
        cls_dict = self.document.__dict__
        if key in cls_dict:
            field = cls_dict[key]
            if isinstance(field, ForeignKey):
                key = cls_dict[key].rel_key
                value = (value and value.pk) or 0
                return (key, value)
            elif isinstance(field, ManyKey):
                key = cls_dict[key].rel_key
                value = cls_dict[key].to_ids(value)
                return (key, value)
            elif isinstance(field, GenerForeignKey):
                vcls = value.__class__
                type = '%s.%s' % (vcls.__module__.rsplit('.', 1)[1], vcls.__name__)
                pk = value.pk
                return [(field.type_field, type), (field.pk_field, pk)]
        return (key + '_id', value.pk)
    
    def _parse_value(self, key, value, is_find=False):
        fields = self.document._meta.fields
        if key in fields:
            field = fields[key]
            if isinstance(field, ArrayField) and is_find:
                return value
            return fields[key].to_value(value)
        return value

class DocumentBase(type):
    def __new__(cls, name, bases, attrs):
        super_new = super(DocumentBase, cls).__new__
        parents = [b for b in bases if isinstance(b, DocumentBase)]
        pattrs = {}
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super_new(cls, name, bases, attrs)
        else:
            for p in parents:
                d = p.__dict__
                for k, v in d.items():
                    if isinstance(v, Field):
                        pattrs[k] = v
            pattrs.update(attrs)
            attrs = pattrs
        # Create the class.
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})
        attr_meta = attrs.pop('Meta', None)
        abstract = getattr(attr_meta, 'abstract', False)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta
        # Add all attributes to the class.
        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)
        new_class.set_meta_options(meta)
        for parent in [cls for cls in parents if hasattr(cls, '_meta')]:
            parent_fields = parent._meta.fields
            for name, field in parent_fields.items():
                new_class.add_to_class(name, field)
        new_class.set_objects_document()
        return new_class
    
    def add_to_class(cls, name, value):
        setattr(cls, name, value)
    
    def set_meta_options(cls, meta):
        _meta = meta()
        _meta.app_name = getattr(meta, 'app_name', None)
        if _meta.app_name is None:
            _meta.app_name = cls.__module__.split('.')[-1]
        _meta.object_name = cls.__name__
        _meta.module_name = _meta.object_name.lower()
        _meta.collection_name = getattr(meta, 'collection_name', None)
        if _meta.collection_name is None:
            _meta.collection_name = '%s_%s' % (_meta.app_name, _meta.module_name)
        #fields
        _meta.fields = {}
        _meta.fields['id'] = IntegerField()
        cls_dict = cls.__dict__
        for k, v in cls_dict.items():
            if isinstance(v, Field):
                v.name = k
                _meta.fields[k] = v
        cls._meta = _meta
        #related_set
        for k, v in cls_dict.items():
            if isinstance(v, ManyKey):
                v.rel_key = v.name or k
                v.cache_name = '_cache_%s' % k
            elif isinstance(v, ForeignKey):
                vdict = v.__dict__
                rel_name = v.related_name or '%s_set' % _meta.module_name
                rel_key = v.name or '%s_id' % k
                v.rel_key = rel_key
                v.cache_name = '_cache_%s' % k
                _meta.fields[rel_key] = IntegerField()
                
                if isinstance(v.rel, str):
                    v.rel = cls
                    rset = ForeignRelated(cls, rel_name, k)
                    setattr(cls, rel_name, rset)
                elif issubclass(v.rel, Document):
                    if getattr(v.rel, rel_name, None) is None:
                        rset = ForeignRelated(cls, rel_name, k)
                        setattr(v.rel, rel_name, rset)
                    else:
                        raise Exception('%s foreign related %s is used' % (v.rel, rel_name))
                else:
                    raise Exception('%s must be Document Subclass' % v.rel)
        #gener foreign key
        for k, v in cls_dict.items():
            if isinstance(v, GenerForeignKey):
                v.set_name(k)
        
    def set_objects_document(cls):
        cls.objects = Manager(cls)

class Document(object):
    __metaclass__ = DocumentBase
    
    def __init__(self, **kwargs):
        cls = self.__class__
        fields = cls._meta.fields
        for k, field in fields.items():
            setattr(self, k, field.get_default())
        self.prepare(**kwargs)
            
    def prepare(self, **kwargs):
        cls = self.__class__
        fields = cls._meta.fields
        for k, v in kwargs.items():
            if k in fields:
                v = fields[k].to_value(v)
            setattr(self, k, v)
            
    def inc(self, name, value):
        spec = {'id':self.id}
        doc = {
               name + '__inc': value
               }
        self.objects.update(spec, **doc)
        
    def incnum(self, **kwargs):
        spec = {'id':self.id}
        doc = {}
        for k, v in kwargs.items():
            doc[k+'__inc'] = v
        self.objects.update(spec, **doc)
        
    def set_value(self, **kwargs):
        spec = {'id':self.id}
        self.objects.update(spec, **kwargs)
    
    @property
    def pk(self):
        try:
            return self.id
        except:
            return None
    
    def save(self):
        origin = self.__class__
        created = not self.pk
        if created:
            self.id = self._get_autoid()
        signals.pre_save.send(sender=origin, instance=self)
        data_copy = self.__dict__.copy()
        result = self.objects.save(**data_copy)
        self._id = result
        signals.post_save.send(sender=origin, instance=self, created=created)
        return result
        
    def delete(self):
        if not self.pk:
            return False
        origin = self.__class__
        signals.pre_delete.send(sender=origin, instance=self)
        cls_dict = self.__class__.__dict__
        for k, v in cls_dict.items():
            if isinstance(v, ForeignRelated):
                spec = {v.rel_field_name:self}
                v.rel.objects.delete(**spec)
        self.objects.remove(id=self.pk)
        signals.post_delete.send(sender=origin, instance=self)
        clear_obj_cache(self)
    
    def id2_id(self, **kwargs):
        if kwargs:
            for k, v in kwargs.items():
                doc = {k+'_id':v._id}
                self.id2value(**doc)
        cls_dict = self.__class__.__dict__
        for k, v in cls_dict.items():
            if isinstance(v, ForeignRelated):
                spec = {v.rel_field_name:self}
                v.rel.objects.id2_id(**spec)
        
    def _get_autoid(self):
        origin = self.__class__
        name = origin._meta.collection_name
        return AutoID.get_id(name)
        
    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.pk == other.pk
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    class Meta:
        pass

class AutoID(Document):
    @classmethod
    def get_id(cls, name):
        query = {'name':name}
        update = {'$inc':{'id':1}}
        wcollection = cls.objects.wcollection()
        data = wcollection.find_and_modify(query, update, upsert=True, new=True)
        return data['id']
    
    @classmethod
    def set_id(cls, name, id):
        spec = {'name':name}
        cls.objects.update(spec, id=id, upsert=True)
    
    class Meta:
        app_name = 'mongo'
    