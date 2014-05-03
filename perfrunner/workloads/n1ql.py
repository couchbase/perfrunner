INDEX_STATEMENTS = {
    'basic': (
        'CREATE INDEX by_city ON {}(city.f.f)',
        'CREATE INDEX by_county ON {}(county.f.f)',
        'CREATE INDEX by_realm ON {}(realm.f)',
    ),
    'range': (
        'CREATE INDEX by_coins ON {}(coins.f)',
        'CREATE INDEX by_achievement ON {}(achievements)',
        'CREATE INDEX by_category ON {}(category)',
        'CREATE INDEX by_year ON {}(year)',
        'CREATE INDEX by_coins ON {}(coins.f)',
    ),
    'multi_emits': (
        'CREATE INDEX by_city ON {}(city.f.f)',
        'CREATE INDEX by_county ON {}(county.f.f)',
        'CREATE INDEX by_country ON {}(country.f)',
    ),
    'body': (
        'CREATE INDEX by_city ON {}(city.f.f)',
        'CREATE INDEX by_realm ON {}(realm.f)',
        'CREATE INDEX by_country ON {}(country.f)',
    ),
    'group_by': (
        'CREATE INDEX by_state ON {}(state.f)',
        'CREATE INDEX by_year ON {}(year)',
        'CREATE INDEX by_gmtime ON {}(gmtime)',
        'CREATE INDEX by_year ON {}(year)',
        'CREATE INDEX by_full_state ON {}(full_state.f)',
        'CREATE INDEX by_year ON {}(year)',
    ),
}
