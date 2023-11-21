create trigger _updated_at_time_public_example_table
    before update on public.example_table
    for each row execute procedure public.update_modified_column();