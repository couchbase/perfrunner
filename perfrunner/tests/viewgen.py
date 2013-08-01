import string


class ResetableGenerator(object):

    def __init__(self, iterable):
        self.reset()
        self.iterable = iterable

    def next(self):
        self.counter += 1
        if self.counter == len(self.iterable):
            self.counter = 0
        return self.iterable[self.counter]

    def reset(self):
        self.counter = -1


class ViewGen(object):

    ddoc_names = ResetableGenerator(tuple(string.ascii_uppercase))

    view_names = ResetableGenerator((
        'id_by_city',
        'name_and_email_by_category_and_and_coins',
        'id_by_realm_and_coins',
        'name_and_email_by_city',
        'name_by_category_and_and_coins',
        'experts_id_by_realm_and_coins',
        'id_by_realm',
        'achievements_by_category_and_and_coins',
        'name_and_email_by_realm_and_coins',
        'experts_coins_by_name'
    ))

    map_funcs = {
        'id_by_city': '''
            function(doc, meta) {
                emit(doc.city, null);
            }
        ''',
        'name_and_email_by_city': '''
            function(doc, meta) {
                emit(doc.city, [doc.name, doc.email]);
            }
        ''',
        'id_by_realm': '''
            function(doc, meta) {
                emit(doc.realm, null);
            }
        ''',
        'experts_coins_by_name': '''
            function(doc, meta) {
                if (doc.category === 2) {
                    emit(doc.name, doc.coins);
                }
            }
        ''',
        'name_by_category_and_and_coins': '''
            function(doc, meta) {
                emit([doc.category, doc.coins], doc.name);
            }
        ''',
        'name_and_email_by_category_and_and_coins': '''
            function(doc, meta) {
                emit([doc.category, doc.coins], [doc.name, doc.email]);
            }
        ''',
        'achievements_by_category_and_and_coins': '''
            function(doc, meta) {
                emit([doc.category, doc.coins], doc.achievements);
            }
        ''',
        'id_by_realm_and_coins': '''
            function(doc, meta) {
                emit([doc.realm, doc.coins], null)
            }
        ''',
        'name_and_email_by_realm_and_coins': '''
            function(doc, meta) {
                emit([doc.realm, doc.coins], [doc.name, doc.email]);
            }
        ''',
        'experts_id_by_realm_and_coins': '''
            function(doc, meta) {
                if (doc.category === 2) {
                    emit([doc.realm, doc.coins], null);
                }
            }
        '''
    }

    def generate_ddocs(self, pattern, options=None):
        """Generate dictionary with design documents and views.
        Pattern looks like:
            [8, 8, 8] -- 8 ddocs (8 views, 8 views, 8 views)
            [2, 2, 4] -- 3 ddocs (2 views, 2 views, 4 views)
            [8] -- 1 ddoc (8 views)
            [1, 1, 1, 1] -- 4 ddocs (1 view per ddoc)
        """
        if filter(lambda v: v > 10, pattern):
            raise Exception('Maximum 10 views per ddoc allowed')
        if len(pattern) > 10:
            raise Exception('Maximum 10 design documents allowed')

        ddocs = dict()
        for number_of_views in pattern:
            ddoc_name = self.ddoc_names.next()
            ddocs[ddoc_name] = {'views': {}}
            for index_of_view in xrange(number_of_views):
                view_name = self.view_names.next()
                map_func = self.map_funcs[view_name]
                ddocs[ddoc_name]['views'][view_name] = {'map': map_func}
            if options:
                ddocs[ddoc_name]['options'] = options

        self.ddoc_names.reset()
        self.view_names.reset()
        return ddocs
