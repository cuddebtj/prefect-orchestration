create table if not exists yahoo_data.example_table(
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    modified_timestamp timestamp without time zone constraint modified_at_constraint default NOW(),
    constraint inserted_at_game_id primary key(game_id, inserted_timestamp)
);