import string


class ResetableGenerator(object):

    def __init__(self, iterable):
        self.counter = -1
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
        'name_and_email_by_category_and_coins',
        'id_by_realm_and_coins',
        'name_and_email_by_city',
        'name_by_category_and_coins',
        'experts_id_by_realm_and_coins',
        'id_by_realm',
        'achievements_by_category_and_coins',
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
        'name_by_category_and_coins': '''
            function(doc, meta) {
                emit([doc.category, doc.coins], doc.name);
            }
        ''',
        'name_and_email_by_category_and_coins': '''
            function(doc, meta) {
                emit([doc.category, doc.coins], [doc.name, doc.email]);
            }
        ''',
        'achievements_by_category_and_coins': '''
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


class ViewGenDev(object):

    MAP_FUNCS = {
        # ############################## BASIC ############################## #
        'basic': {
            'name_and_street_by_city': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            doc.city.f.f,
                            [
                                doc.name.f.f.f,
                                doc.street.f.f
                            ]
                        );
                    }
                ''',
            },
            'name_and_email_by_county': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            doc.county.f.f,
                            [
                                doc.name.f.f.f,
                                doc.email.f.f
                            ]
                        );
                    }
                ''',
            },
            'achievements_by_realm': {
                'map': '''
                    function(doc, meta) {
                        emit(doc.realm.f, doc.achievements);
                    }
                ''',
            },
        },
        # ############################## RANGE ############################## #
        'range': {
            'name_by_coins': {
                'map': '''
                    function(doc, meta) {
                        emit(doc.coins.f, doc.name.f.f.f);
                    }
                ''',
            },
            'email_by_achievement_and_category': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            [
                                doc.achievements[0],
                                doc.category
                            ],
                            doc.email.f.f
                        );
                    }
                ''',
            },
            'street_by_year_and_coins': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            [
                                doc.year,
                                doc.coins.f
                            ],
                            doc.street.f.f
                        );
                    }
                ''',
            },
        },
        # ############################## GROUP ############################## #
        'group_by': {
            'coins_stats_by_state_and_year': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            [
                                doc.state.f,
                                doc.year
                            ],
                            doc.coins.f
                        );
                    }
                ''',
                'reduce': '_stats',
            },
            'coins_stats_by_gmtime_and_year': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            [doc.gmtime, doc.year],
                            doc.coins.f
                        );
                    }
                ''',
                'reduce': '_stats',
            },
            'coins_stats_by_full_state_and_year': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            [
                                doc.full_state.f,
                                doc.year
                            ],
                            doc.coins.f
                        );
                    }
                ''',
                'reduce': '_stats',
            },
        },
        # ############################## MULTI ############################## #
        'multi_emits': {
            'name_and_email_and_street_and_achievements_and_coins_by_city': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            doc.city.f.f,
                            [
                                doc.name.f.f.f,
                                doc.email.f.f,
                                doc.street.f.f,
                                doc.achievements,
                                doc.coins.f
                            ]
                        );
                    }
                ''',
            },
            'street_and_name_and_email_and_achievement_and_coins_by_county': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            doc.city.f.f,
                            [
                                doc.street.f.f,
                                doc.name.f.f.f,
                                doc.email.f.f,
                                doc.achievements[0],
                                2 * doc.coins.f
                            ]
                        );
                    }
                ''',
            },
            'category_name_and_email_and_street_and_gmtime_and_year_by_country': {
                'map': '''
                    function(doc, meta) {
                        emit(
                            doc.city.f.f,
                            [
                                doc.category,
                                doc.name.f.f.f,
                                doc.email.f.f,
                                doc.street.f.f,
                                doc.gmtime,
                                doc.year
                            ]
                        );
                    }
                ''',
            },
        },
        # ############################# COMPUTE ############################# #
        'compute': {
            'calc_by_city': {
                'map': '''
                    function(doc, meta) {
                        var calc = [];
                        for (var i = 0; i < doc.achievements.length; i++) {
                            for (var j = 0; j < doc.gmtime.length; j++) {
                                var x = Math.random() * Math.exp(i + j),
                                    y = Math.sin(doc.achievements[i]),
                                    z = Math.cos(doc.gmtime[j]);
                                var agg = Math.round(x * (y - z));
                                if (agg < 0) {
                                    calc.push(agg);
                                }
                            }
                        }
                        emit(doc.city.f.f, calc);
                    }
                ''',
            },
            'calc_by_county': {
                'map': '''
                    function(doc, meta) {
                        var calc = [];
                        for (var i = 0; i < doc.achievements.length; i++) {
                            for (var j = 0; j < doc.gmtime.length; j++) {
                                var x = Math.random() * Math.exp(i + j),
                                    y = Math.tan(doc.achievements[i]),
                                    z = Math.sqrt(doc.gmtime[j]);
                                var agg = Math.round(x * (y - z));
                                if (agg > 0) {
                                    calc.push(agg);
                                }
                            }
                        }
                        emit(doc.county.f.f, calc);
                    }
                ''',
            },
            'calc_by_realm': {
                'map': '''
                    function(doc, meta) {
                        var calc = [];
                        for (var i = 0; i < doc.achievements.length; i++) {
                            for (var j = 0; j < doc.gmtime.length; j++) {
                                var x = Math.random() * Math.pow(i + 1, j + 1),
                                    y = Math.tan(doc.achievements[i]),
                                    z = Math.cos(doc.gmtime[j]);
                                var agg = Math.round(x / (y + z));
                                if (agg > 0) {
                                    calc.push(agg);
                                }
                            }
                        }
                        emit(doc.county.f.f, calc);
                    }
                ''',
            },
        },
        # ############################## BODY ############################## #
        'body': {
            'body_by_city': {
                'map': '''
                    function(doc, meta) {
                        emit(doc.city.f.f, doc.body);
                    }
                ''',
            },
            'body_by_realm': {
                'map': '''
                    function(doc, meta) {
                        emit(doc.realm.f, doc.body);
                    }
                ''',
            },
            'body_by_country': {
                'map': '''
                    function(doc, meta) {
                        emit(doc.country.f, doc.body);
                    }
                ''',
            },
        }
    }

    def generate_ddocs(self, index_type):
        """Return ddoc definition based on index type"""
        return {
            'ddoc': {
                'views': self.MAP_FUNCS[index_type]
            }
        }
