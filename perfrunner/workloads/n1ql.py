INDEX_STATEMENTS = {
    'name_and_street_by_city': (
        'CREATE INDEX by_city ON {}(city.f.f)',
    ),
    'name_and_email_by_county': (
        'CREATE INDEX by_county ON {}(county.f.f)',
    ),
    'achievements_by_realm': (
        'CREATE INDEX by_realm ON {}(realm.f)',
    ),
    'name_by_coins': (
        'CREATE INDEX by_coins ON {}(coins.f)',
    ),
    'email_by_achievement_and_category': (
        'CREATE INDEX by_achievement ON {}(achievements)',
        'CREATE INDEX by_category ON {}(category)',
    ),
    'street_by_year_and_coins': (
        'CREATE INDEX by_year ON {}(year)',
        'CREATE INDEX by_coins ON {}(coins.f)',
    ),
    'name_and_email_and_street_and_achievements_and_coins_by_city': (
        'CREATE INDEX by_city ON {}(city.f.f)',
    ),
    'street_and_name_and_email_and_achievement_and_coins_by_county': (
        'CREATE INDEX by_county ON {}(county.f.f)',
    ),
    'category_name_and_email_and_street_and_gmtime_and_year_by_country': (
        'CREATE INDEX by_country ON {}(country.f)',
    ),
    'body_by_city': (
        'CREATE INDEX by_city ON {}(city.f.f)',
    ),
    'body_by_realm': (
        'CREATE INDEX by_realm ON {}(realm.f)',
    ),
    'body_by_country': (
        'CREATE INDEX by_country ON {}(country.f)',
    ),
    'coins_stats_by_state_and_year': (
        'CREATE INDEX by_state ON {}(state.f)',
        'CREATE INDEX by_year ON {}(year)',
    ),
    'coins_stats_by_gmtime_and_year': (
        'CREATE INDEX by_gmtime ON {}(gmtime)',
        'CREATE INDEX by_year ON {}(year)',
    ),
    'coins_stats_by_full_state_and_year': (
        'CREATE INDEX by_full_state ON {}(full_state.f)',
        'CREATE INDEX by_year ON {}(year)',
    ),
}
